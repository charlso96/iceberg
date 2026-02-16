/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iceberg.exp;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.nio.file.StandardOpenOption.APPEND;
import static java.nio.file.StandardOpenOption.CREATE;
import static org.apache.iceberg.types.Types.NestedField.required;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.File2;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.aws.AwsClientFactories;
import org.apache.iceberg.aws.AwsClientProperties;
import org.apache.iceberg.aws.s3.S3FileIO;
import org.apache.iceberg.aws.s3.S3FileIOProperties;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hive.HiveCatalog;
import org.apache.iceberg.hive.HiveCatalog2;
import org.apache.iceberg.raven.CatalogOuterClass;
import org.apache.iceberg.raven.CatalogOuterClass.FileObject;
import org.apache.iceberg.raven.CatalogOuterClass.TableObject;
import org.apache.iceberg.raven.RavenCatalog;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Types;
import org.duckdb.DuckDBConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;

public class MultiTableTxn2 {
    // 1. Add this private constructor
    private MultiTableTxn2() {}

    public static class MetricsExporter {
        public static void exportMetricsToLog(String outputFile) {
            ObjectMapper mapper = new ObjectMapper();
            mapper.registerModule(new JavaTimeModule());
            mapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

            // IMPORTANT: Disable pretty printing.
            // NDJSON requires each object to be on exactly one line.
            // If you pretty print, the newlines inside the object will break the format.
            try (BufferedWriter writer = Files.newBufferedWriter(Paths.get(outputFile), UTF_8)) {
                int size = LOAD_TABLE_TIMES.size();

                for (int i = 0; i < size; i++) {
                    ObjectNode row = mapper.createObjectNode();

                    // 1. Build the object
                    addTimeBlock(mapper, row, "load_table", LOAD_TABLE_TIMES.get(i));
                    addTimeBlock(mapper, row, "insert_file", INSERT_FILE_TIMES.get(i));
                    addTimeBlock(mapper, row, "commit", COMMIT_TIMES.get(i));

                    ArrayNode filesArray = mapper.createArrayNode();
                    List<File2> fileList = ADDED_FILES.get(i);
                    if (fileList != null) {
                        for (File2 file : fileList) {
                            ObjectNode fileObj = mapper.createObjectNode();
                            fileObj.put("type", file.fileType().name().toLowerCase(Locale.getDefault()));
                            fileObj.put("path", file.path());
                            fileObj.put("tag", file.tag());
                            fileObj.put("size", getFileSize(file.path()));
                            filesArray.add(fileObj);
                        }
                    }
                    row.set("files", filesArray);

                    // 2. Write as single-line JSON string
                    String jsonLine = mapper.writeValueAsString(row);

                    // 3. Write to file + Newline
                    writer.write(jsonLine);
                    writer.newLine(); // This makes it valid NDJSON
                }
            }
            catch (IOException e) {
                LOG.info("IO Error during result output", e);
            }

        }

        public static void appendSummaryToJson(String outputFile) {
            ObjectMapper mapper = new ObjectMapper();

            // 2. Calculate num_files (Aggregating all files of type ADD)
            long numFiles = 0;
            for (List<File2> fileList : ADDED_FILES) {
                if (fileList != null) {
                    numFiles += fileList.stream()
                            .filter(f -> f.fileType() == File2.File2Type.ADD)
                            .count();
                }
            }

            // 3. Calculate avg_latency
            double totalLatencyMillis = 0;

            for (int i = 0; i < numTxn; i++) {
                // Start: Always LOAD_TABLE start
                Instant start = LOAD_TABLE_TIMES.get(i).start;

                // End: COMPACT end if available, otherwise COMMIT end
                Instant end = COMMIT_TIMES.get(i).end;

                // Calculate duration in milliseconds
                long duration = Duration.between(start, end).toMillis();
                totalLatencyMillis += duration;
            }

            double avgLatency = (numTxn == 0) ? 0 : (totalLatencyMillis / numTxn);

            // 4. Construct the JSON Object
            ObjectNode summaryNode = mapper.createObjectNode();
            summaryNode.put("exp_name", "MultiTableTxn2");
            summaryNode.put("exp_type", expType);
            summaryNode.put("num_tables", numTables);
            summaryNode.put("num_txn", numTxn);
            summaryNode.put("num_files", numFiles);
            summaryNode.put("avg_latency", avgLatency);

            // 5. Append to file
            // 'true' in FileWriter constructor enables append mode
            try (BufferedWriter writer = Files.newBufferedWriter(Paths.get(outputFile), UTF_8, CREATE, APPEND)) {
                String jsonLine = mapper.writeValueAsString(summaryNode);
                writer.write(jsonLine);
                writer.newLine(); // Add newline so it remains valid NDJSON
            } catch (IOException e) {
                LOG.info("IO Error during result output", e);
            }
        }

        private static void addTimeBlock(ObjectMapper mapper, ObjectNode parent, String fieldName, TimePair timePair) {
            if (timePair == null) return;
            ObjectNode timeNode = mapper.createObjectNode();
            timeNode.put("start", timePair.start.toString());
            timeNode.put("end", timePair.end.toString());
            parent.set(fieldName, timeNode);
        }
    }


    public static class TimePair {
        public final Instant start;
        public final Instant end;

        public TimePair(Instant start, Instant end) {
            this.start = start;
            this.end = end;
        }

        public long getDurationMillis() {
            return end.toEpochMilli() - start.toEpochMilli();
        }

        // Getters, equals(), hashCode(), and toString() would go here
    }

    private static final Logger LOG = LoggerFactory.getLogger(MultiTableTxn2.class);
    private static final ObjectMapper JSON_MAPPER = new ObjectMapper();
    private static String expType;
    private static String workspaceName;
    private static String dbName;
    private static int numTxn;
    private static int numTables;
    private static int numRowsPerFile;
    private static String warehouseLocation;
    private static String s3Secret;
    private static String s3KeyId;
    private static String s3Region;
    private static S3Client s3;
    private static String ravenAddress;

    // hive catalog
    private static HiveCatalog catalog;
    private static HiveCatalog2 catalog2;
    private static RavenCatalog ravenCatalog;
    private static DuckDBConnection duckDbConn;

    // data structures for measurements
    private static final List<TimePair> LOAD_TABLE_TIMES = Lists.newArrayList();
    private static final List<TimePair> INSERT_FILE_TIMES = Lists.newArrayList();
    private static final List<TimePair> COMMIT_TIMES = Lists.newArrayList();
    private static final List<List<File2>> ADDED_FILES = Lists.newArrayList();

    public static Map<String, String> parseJsonToMap(String jsonFilePath) throws IOException {
        File file = new File(jsonFilePath);

        // TypeReference is essential to tell Jackson the specific
        // Map implementation and generic types to use.
        return JSON_MAPPER.readValue(file, new TypeReference<Map<String, String>>() {});
    }

    public static void initCatalog() throws Exception {
        // load the hive config
        HiveConf hiveConf = new HiveConf();

        Map<String, String> conf = Maps.newHashMap();
        conf.put(CatalogProperties.URI, "thrift://localhost:9083");
        conf.put(
                CatalogProperties.CLIENT_POOL_CACHE_EVICTION_INTERVAL_MS,
                String.valueOf(TimeUnit.SECONDS.toMillis(10)));
        conf.put(CatalogProperties.WAREHOUSE_LOCATION, warehouseLocation);
        conf.put(CatalogProperties.FILE_IO_IMPL, S3FileIO.class.getName());
        conf.put(S3FileIOProperties.ACCESS_KEY_ID, s3KeyId);
        conf.put(S3FileIOProperties.SECRET_ACCESS_KEY, s3Secret);
        conf.put(AwsClientProperties.CLIENT_REGION, s3Region);
        conf.put("raven_address", ravenAddress);
        conf.put("workspace_name", workspaceName);
        //        conf.put(S3FileIOProperties.SESSION_TOKEN, StaticClientFactory.class.getName());
        // for accessing s3 later on
        AwsClientFactories.defaultFactory().initialize(conf);
        s3 = AwsClientFactories.defaultFactory().s3();
        catalog = null;
        catalog2 = null;
        if (expType.equals("raven")) {
            catalog2 =
                    (HiveCatalog2)
                            CatalogUtil.loadCatalog(
                                    HiveCatalog2.class.getName(),
                                    CatalogUtil.ICEBERG_CATALOG_TYPE_HIVE,
                                    conf, hiveConf);
        }
        else {
            catalog =
                    (HiveCatalog)
                            CatalogUtil.loadCatalog(
                                    HiveCatalog.class.getName(),
                                    CatalogUtil.ICEBERG_CATALOG_TYPE_HIVE,
                                    conf, hiveConf);
        }


    }

    public static void main(String[] args) throws Exception {
        // System.setProperty("datanucleus.plugin.pluginRegistryBundleCheck", "LOG");
        // read the config file for experimentation
        Map<String, String> expConfigs = parseJsonToMap(args[0]);
        expType = expConfigs.get("exp_type");
        workspaceName = expConfigs.get("workspace_name");
        dbName = expConfigs.get("db_name");
        numTxn = Integer.parseInt(expConfigs.get("num_txn"));
        numTables = Integer.parseInt(expConfigs.get("num_tables"));
        numRowsPerFile = Integer.parseInt(expConfigs.get("num_rows_per_file"));
        warehouseLocation = expConfigs.get("warehouse_location");
        s3Secret = expConfigs.get("s3_secret");
        s3KeyId = expConfigs.get("s3_key_id");
        s3Region = expConfigs.get("s3_region");
        ravenAddress = expConfigs.get("raven_address");

        initCatalog();
        duckDbConn = (DuckDBConnection) DriverManager.getConnection("jdbc:duckdb:");

        try (Statement stmt = duckDbConn.createStatement()) {
            stmt.execute("INSTALL httpfs; LOAD httpfs;");
            stmt.execute("INSTALL parquet; LOAD parquet;");
            String secretSql =
                    String.format(
                            Locale.getDefault(),
                            "CREATE SECRET IF NOT EXISTS s3_secret (TYPE S3, KEY_ID '%s', SECRET '%s', REGION '%s');",
                            s3KeyId,
                            s3Secret,
                            s3Region);
            stmt.execute(secretSql);
        }

        if (expType.equals("raven")) {
            runRavenExp(expConfigs);
        } else {
            runVanillaExp(expConfigs);
        }

    }

    private static String schemaToTargetList(Schema schema) {
        List<Types.NestedField> columns = schema.columns();
        StringBuilder sb = new StringBuilder();
        for (Types.NestedField column : columns) {
            switch (column.type().typeId()) {
                case INTEGER:
                    sb.append(String.format(Locale.getDefault(), "x::INTEGER AS %s,", column.name()));
                    break;
                case STRING:
                    sb.append(String.format(Locale.getDefault(), "x::VARCHAR AS %s,", column.name()));
                    break;
                default:
                    break;
            }
        }
        // remove the last comma
        if (sb.length() > 0) {
            sb.setLength(sb.length() - 1);
        }

        return sb.toString();
    }

    private static Metrics computeStats(Schema schema) {
        Map<Integer, Long> valueCounts = Maps.newHashMap();
        Map<Integer, Long> nullValueCounts = Maps.newHashMap();
        Map<Integer, Long> nanValueCounts = Maps.newHashMap();
        Map<Integer, ByteBuffer> lowerBounds = Maps.newHashMap();
        Map<Integer, ByteBuffer> upperBounds = Maps.newHashMap();

        ByteBuffer lowerBoundInt = ByteBuffer.allocate(Integer.BYTES);
        lowerBoundInt.order(ByteOrder.LITTLE_ENDIAN); // Set byte order
        lowerBoundInt.putInt(1);
        lowerBoundInt.rewind();

        ByteBuffer upperBoundInt = ByteBuffer.allocate(Integer.BYTES);
        upperBoundInt.order(ByteOrder.LITTLE_ENDIAN); // Set byte order
        upperBoundInt.putInt(numRowsPerFile);
        upperBoundInt.rewind();

        ByteBuffer lowerBoundStr = ByteBuffer.wrap(String.valueOf(1).getBytes(StandardCharsets.UTF_8));
        ByteBuffer upperBoundStr = ByteBuffer.wrap(String.valueOf(numRowsPerFile).getBytes(StandardCharsets.UTF_8));

        List<Types.NestedField> columns = schema.columns();
        for (Types.NestedField column : columns) {
            valueCounts.put(column.fieldId(), (long) numRowsPerFile);
            nullValueCounts.put(column.fieldId(), 0L);
            nanValueCounts.put(column.fieldId(), 0L);
            switch (column.type().typeId()) {
                case INTEGER:
                    lowerBounds.put(column.fieldId(), lowerBoundInt);
                    upperBounds.put(column.fieldId(), upperBoundInt);
                    break;
                case STRING:
                    lowerBounds.put(column.fieldId(), lowerBoundStr);
                    upperBounds.put(column.fieldId(), upperBoundStr);
                    break;
                default:
                    break;
            }
        }

        return new Metrics((long) numRowsPerFile, null, valueCounts, nullValueCounts,
                nanValueCounts, lowerBounds, upperBounds);
    }

    private static void runRavenExp(Map<String, String> expConfigs) {
        // create retail database
        catalog2.createNamespace(Namespace.of(dbName));
        // create all the tpcds tables
        for (int i = 0; i < TPCDSSchema.SCHEMA_LIST.size(); i++) {
            Schema schema = TPCDSSchema.SCHEMA_LIST.get(i);
            String tableName = TPCDSSchema.SCHEMA_NAMES.get(i);
            String location = String.format("%s/%s.db/%s", warehouseLocation, dbName, tableName);
            PartitionSpec spec = PartitionSpec.builderFor(schema).build();
            TableIdentifier tableIdent = TableIdentifier.of(dbName, tableName);
            catalog2.createTable(tableIdent, schema, spec, location, ImmutableMap.of());
        }

        ravenCatalog = new RavenCatalog(ravenAddress);
        runRavenExpImpl();

        String expResultDir = expConfigs.get("exp_result_dir");
        String logFileName = String.format(Locale.getDefault(),"%s/multitabletxn2-iceberg-raven-%d-%d-log.json",
                expResultDir, numTables, numRowsPerFile);
        String summaryFileName = String.format("%s/summary.json", expResultDir);
        MetricsExporter.exportMetricsToLog(logFileName);
        MetricsExporter.appendSummaryToJson(summaryFileName);
    }

    private static void runVanillaExp(Map<String, String> expConfigs) {
        // create retail database
        catalog.createNamespace(Namespace.of(dbName));
        // create all the tpcds tables
        for (int i = 0; i < TPCDSSchema.SCHEMA_LIST.size(); i++) {
            Schema schema = TPCDSSchema.SCHEMA_LIST.get(i);
            String tableName = TPCDSSchema.SCHEMA_NAMES.get(i);
            String location = String.format("%s/%s.db/%s", warehouseLocation, dbName, tableName);
            PartitionSpec spec = PartitionSpec.builderFor(schema).build();
            TableIdentifier tableIdent = TableIdentifier.of(dbName, tableName);
            catalog.createTable(tableIdent, schema, spec, location, ImmutableMap.of());
        }

        runVanillaExpImpl();

        String expResultDir = expConfigs.get("exp_result_dir");
        String logFileName = String.format(Locale.getDefault(),"%s/multitabletxn2-iceberg-vanilla-%d-%d-log.json",
                expResultDir, numTables, numRowsPerFile);
        String summaryFileName = String.format("%s/summary.json", expResultDir);
        MetricsExporter.exportMetricsToLog(logFileName);
        MetricsExporter.appendSummaryToJson(summaryFileName);
    }

    private static void runRavenExpImpl() {
        // Keep running until flag change
        for (int i = 0; i < numTxn; i++) {
            try (Statement stmt = duckDbConn.createStatement()) {
                // first pick random tables to write to
                List<Integer> tableIdxList = ThreadLocalRandom.current()
                        .ints(0, TPCDSSchema.SCHEMA_LIST.size())
                        .distinct()      // Filters out duplicates
                        .limit(numTables)        // Stops once we have exactly 'n' distinct numbers
                        .boxed()         // Converts primitive int to Integer
                        .collect(Collectors.toList());
                List<List<File2>> fileLogs = Lists.newArrayList();

                Instant beforeLoadTables = Instant.now();

                List<String> tableNames = Lists.newArrayList();
                for (Integer tableIdx : tableIdxList) {
                    tableNames.add(TPCDSSchema.SCHEMA_NAMES.get(tableIdx));
                }

                // start txn on raven
                CatalogOuterClass.StartTransactionResponse ravenTxn = ravenCatalog.startTransaction(
                        workspaceName, dbName, tableNames);

                long txnId = ravenTxn.getTxnId();
                List<TableObject> unsortedTableObjects = ravenTxn.getTablesList();

                List<TableObject> tableObjects = Lists.newArrayList();
                List<Table> tables = Lists.newArrayList();
                List<AppendFiles> txns = Lists.newArrayList();
                for (Integer tableIdx : tableIdxList) {
                    String tableName = TPCDSSchema.SCHEMA_NAMES.get(tableIdx);
                    Table table = catalog2.loadTable(TableIdentifier.of(dbName, tableName));
                    AppendFiles txn = table.newFastAppend();
                    // brute force way to sort table objects. Should not take too long
                    Optional<TableObject> tableObject = unsortedTableObjects.stream()
                            .filter(t -> t.getTableName().equals(tableName)) // The Predicate
                            .findFirst();
                    tableObjects.add(tableObject.get());
                    tables.add(table);
                    txns.add(txn);
                }

                Instant afterLoadTables = Instant.now();

                List<String> filePaths = Lists.newArrayList();
                List<Long> fileSizes = Lists.newArrayList();
                List<Metrics> stats = Lists.newArrayList();
                for (int j = 0 ; j < tableIdxList.size(); j++) {
                    Table table = tables.get(j);
                    Schema schema = TPCDSSchema.SCHEMA_LIST.get(tableIdxList.get(j));
                    String targetList = schemaToTargetList(schema);

                    // generate data
                    stmt.execute(
                            String.format(
                                    Locale.getDefault(),
                                    "CREATE TEMP TABLE staging_data AS SELECT %s FROM generate_series(1, %d) AS t(x);",
                                    targetList,
                                    numRowsPerFile));

                    // construct file statistics
                    Metrics metrics = computeStats(schema);
                    stats.add(metrics);

                    String filePath = String.format("%s/%s.parquet", table.location(), UUID.randomUUID());
                    stmt.execute(
                            String.format(
                                    Locale.getDefault(), "COPY staging_data TO '%s' (FORMAT PARQUET);", filePath));
                    stmt.execute("DROP TABLE staging_data;");

                    filePaths.add(filePath);
                    List<File2> newFileLogs = Lists.newArrayList();
                    fileLogs.add(newFileLogs);
                    fileLogs.get(j).add(new File2(filePath, File2.File2Type.ADD, "data"));

                    long fileSize = getFileSize(filePath);
                    fileSizes.add(fileSize);
                }

                Instant afterInsertFiles = Instant.now();

                for (int j = 0 ; j < tableIdxList.size(); j++) {
                    Schema schema = TPCDSSchema.SCHEMA_LIST.get(tableIdxList.get(j));
                    PartitionSpec spec = PartitionSpec.builderFor(schema).build();

                    DataFile newFile =
                            DataFiles.builder(spec)
                                    .withFileSizeInBytes(fileSizes.get(j))
                                    .withFormat(FileFormat.PARQUET)
                                    .withMetrics(stats.get(j))
                                    .withPath(filePaths.get(j))
                                    .build();

                    txns.get(j).appendFile(newFile);
                    List<File2> curFileLogs = fileLogs.get(j);
                    txns.get(j).commit2(curFileLogs);

                    List<FileObject> newDataFiles = Lists.newArrayList();
                    List<String> filesToReplace = Lists.newArrayList();

                    for (File2 file : curFileLogs) {
                        switch (file.fileType()) {
                            case ADD:
                                FileObject newFileObject = FileObject.newBuilder()
                                        .setTag(file.tag()).setPath(file.path()).build();
                                newDataFiles.add(newFileObject);
                                break;
                            case DELETE:
                                filesToReplace.add(file.path());
                                break;
                            default:
                                break;
                        }
                    }

                    ravenCatalog.finalRewriteFiles(tableObjects.get(j), filesToReplace, newDataFiles, txnId);
                }
                ravenCatalog.commit(txnId);

                Instant afterCommit = Instant.now();

                LOAD_TABLE_TIMES.add(new TimePair(beforeLoadTables, afterLoadTables));
                INSERT_FILE_TIMES.add(new TimePair(afterLoadTables, afterInsertFiles));
                COMMIT_TIMES.add(new TimePair(afterInsertFiles, afterCommit));
                // flatten the fileLogs
                ADDED_FILES.add(fileLogs.stream()
                        .flatMap(List::stream) // or simply Collection::stream
                        .collect(Collectors.toList()));
            } catch (SQLException e) {
                LOG.info("Database error occurred", e);
            }
        }

    }

    private static void runVanillaExpImpl() {
        // run for fixed number of iterations
        for (int i = 0; i < numTxn; i++) {
            try (Statement stmt = duckDbConn.createStatement()) {
                // first pick random tables to write to
                List<Integer> tableIdxList = ThreadLocalRandom.current()
                        .ints(0, TPCDSSchema.SCHEMA_LIST.size())
                        .distinct()      // Filters out duplicates
                        .limit(numTables)        // Stops once we have exactly 'n' distinct numbers
                        .boxed()         // Converts primitive int to Integer
                        .collect(Collectors.toList());
                List<File2> fileLogs = Lists.newArrayList();

                Instant beforeLoadTables = Instant.now();

                List<Table> tables = Lists.newArrayList();
                List<AppendFiles> txns = Lists.newArrayList();
                for (Integer tableIdx : tableIdxList) {
                    String tableName = TPCDSSchema.SCHEMA_NAMES.get(tableIdx);
                    Table table = catalog.loadTable(TableIdentifier.of(dbName, tableName));
                    AppendFiles txn = table.newFastAppend();

                    tables.add(table);
                    txns.add(txn);
                }

                Instant afterLoadTables = Instant.now();

                List<String> filePaths = Lists.newArrayList();
                List<Long> fileSizes = Lists.newArrayList();
                List<Metrics> stats = Lists.newArrayList();
                for (int j = 0 ; j < tableIdxList.size(); j++) {
                    Table table = tables.get(j);
                    Schema schema = TPCDSSchema.SCHEMA_LIST.get(tableIdxList.get(j));
                    String targetList = schemaToTargetList(schema);

                    // generate data
                    stmt.execute(
                            String.format(
                                    Locale.getDefault(),
                                    "CREATE TEMP TABLE staging_data AS SELECT %s FROM generate_series(1, %d) AS t(x);",
                                    targetList,
                                    numRowsPerFile));

                    // construct file statistics
                    Metrics metrics = computeStats(schema);
                    stats.add(metrics);

                    String filePath = String.format("%s/%s.parquet", table.location(), UUID.randomUUID());
                    stmt.execute(
                            String.format(
                                    Locale.getDefault(), "COPY staging_data TO '%s' (FORMAT PARQUET);", filePath));
                    stmt.execute("DROP TABLE staging_data;");

                    filePaths.add(filePath);
                    fileLogs.add(new File2(filePath, File2.File2Type.ADD, "data"));

                    long fileSize = getFileSize(filePath);
                    fileSizes.add(fileSize);
                }

                Instant afterInsertFiles = Instant.now();

                for (int j = 0 ; j < tableIdxList.size(); j++) {
                    Schema schema = TPCDSSchema.SCHEMA_LIST.get(tableIdxList.get(j));
                    PartitionSpec spec = PartitionSpec.builderFor(schema).build();

                    DataFile newFile =
                            DataFiles.builder(spec)
                                    .withFileSizeInBytes(fileSizes.get(j))
                                    .withFormat(FileFormat.PARQUET)
                                    .withMetrics(stats.get(j))
                                    .withPath(filePaths.get(j))
                                    .build();

                    txns.get(j).appendFile(newFile);

                    txns.get(j).commit2(fileLogs);
                }

                Instant afterCommit = Instant.now();

                LOAD_TABLE_TIMES.add(new TimePair(beforeLoadTables, afterLoadTables));
                INSERT_FILE_TIMES.add(new TimePair(afterLoadTables, afterInsertFiles));
                COMMIT_TIMES.add(new TimePair(afterInsertFiles, afterCommit));
                ADDED_FILES.add(fileLogs);
            } catch (SQLException e) {
                LOG.info("Database error occurred", e);
            }
        }
    }

    private static long getFileSize(String path) {
        URI uri = URI.create(path);
        String bucket = uri.getHost();
        String key = uri.getPath().substring(1);
        HeadObjectRequest headRequest = HeadObjectRequest.builder().bucket(bucket).key(key).build();
        return s3.headObject(headRequest).contentLength();
    }

}
