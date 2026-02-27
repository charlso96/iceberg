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
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.Schema;
import org.apache.iceberg.aws.AwsClientFactories;
import org.apache.iceberg.aws.AwsClientProperties;
import org.apache.iceberg.aws.s3.S3FileIO;
import org.apache.iceberg.aws.s3.S3FileIOProperties;
import org.apache.iceberg.raven.CatalogOuterClass.FileObject;
import org.apache.iceberg.raven.CatalogOuterClass.TableObject;
import org.apache.iceberg.raven.RavenCatalog;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Types;
import org.duckdb.DuckDBConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.Delete;
import software.amazon.awssdk.services.s3.model.DeleteObjectsRequest;
import software.amazon.awssdk.services.s3.model.ObjectIdentifier;
import software.amazon.awssdk.services.s3.model.S3Exception;

public class GC1 {
    // 1. Add this private constructor
    private GC1() {}

    public static class RandomOpGenerator {
        private final float x;
        private final float total; // Precomputed total sum (x + y + z)

        public RandomOpGenerator(float x, float y) {
            if (x < 0 || y < 0 || (x + y) == 0.0f) {
                throw new IllegalArgumentException("Weights must be non-negative and sum to greater than 0");
            }

            // Precomputing these bounds saves CPU cycles during the next() calls
            this.x = x;
            this.total = x + y;
        }

        /**
         * Generates the next weighted random number (0, 1, or 2).
         * This method is 100% thread-safe and lock-free.
         */
        public int next() {
            // Generate a random float between 0.0 (inclusive) and total (exclusive)
            float r = ThreadLocalRandom.current().nextFloat() * total;

            // Find which bucket the random number fell into
            if (r < x) {
                return 0; // Hit the 'x' probability slice
            } else {
                return 1; // Hit the 'y' probability slice
            }
        }
    }

    public static class MetricsExporter {
        public static void exportMetricsToLog(String outputFile) {
            ObjectMapper mapper = new ObjectMapper();
            mapper.registerModule(new JavaTimeModule());
            mapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

            // IMPORTANT: Disable pretty printing.
            // NDJSON requires each object to be on exactly one line.
            // If you pretty print, the newlines inside the object will break the format.
            try (BufferedWriter writer = Files.newBufferedWriter(Paths.get(outputFile), UTF_8)) {
                for (int i = 0; i < LOAD_TABLE_TIMES.size(); i++) {
                    for (int j = 0; j < LOAD_TABLE_TIMES.get(i).size(); j++) {
                        // 1. Build the object
                        ObjectNode row = mapper.createObjectNode();
                        row.put("threadNum", i);
                        row.put("success", OP_SUCCESS.get(i).get(j));
                        row.put("type", OP_TYPES.get(i).get(j));
                        addTimeBlock(mapper, row, "load_table", LOAD_TABLE_TIMES.get(i).get(j));
                        addTimeBlock(mapper, row, "insert_file", INSERT_FILE_TIMES.get(i).get(j));
                        addTimeBlock(mapper, row, "commit", COMMIT_TIMES.get(i).get(j));

                        // 2. Write as single-line JSON string
                        String jsonLine = mapper.writeValueAsString(row);
                        // 3. Write to file + Newline
                        writer.write(jsonLine);
                        writer.newLine(); // This makes it valid NDJSON
                    }
                }
            }
            catch (IOException e) {
                LOG.info("IO Error during result output", e);
            }

        }

        public static void appendSummaryToJson(String outputFile) {
            ObjectMapper mapper = new ObjectMapper();

            // 1. Calculate num_txn
            int numTxn = 0;
            for (List<TimePair> loadTimes : LOAD_TABLE_TIMES) {
                numTxn += loadTimes.size();
            }

            // 3. Calculate avg_latency
            double totalLatencyMillis = 0;

            for (int i = 0; i < LOAD_TABLE_TIMES.size(); i++) {
                for (int j = 0; j < LOAD_TABLE_TIMES.get(i).size(); j++) {
                    // Start: Always LOAD_TABLE start
                    Instant start = LOAD_TABLE_TIMES.get(i).get(j).start;

                    // End: COMPACT end if available, otherwise COMMIT end
                    Instant end = COMMIT_TIMES.get(i).get(j).end;

                    // Calculate duration in milliseconds
                    long duration = Duration.between(start, end).toMillis();
                    totalLatencyMillis += duration;
                }
            }

            int numSuccess = 0;
            int numFailure = 0;
            for (List<Boolean> opSuccess : OP_SUCCESS) {
                for (Boolean success : opSuccess) {
                    if (success) {
                        numSuccess += 1;
                    }
                    else {
                        numFailure += 1;
                    }
                }
            }

            double avgLatency = (numTxn == 0) ? 0 : (totalLatencyMillis / numTxn);

            // 4. Construct the JSON Object
            ObjectNode summaryNode = mapper.createObjectNode();
            summaryNode.put("exp_name", "gc1");
            summaryNode.put("duration", durationStr);
            summaryNode.put("num_txn", numTxn);
            summaryNode.put("num_success", numSuccess);
            summaryNode.put("num_failure", numFailure);
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

    }

    private static final Logger LOG = LoggerFactory.getLogger(GC1.class);
    private static final ObjectMapper JSON_MAPPER = new ObjectMapper();
    private static final int NUM_TABLES = 10000;

    private static float refreshRatio;
    private static float insertRatio;

    private static String durationStr;
    private static String workspaceName;
    private static String dbName;
    private static String tableName;
    private static int numThreads;
    // unlike MORWrite, compaction merges small files to a large file & expire snapshots
    private static int txnPerCompaction;
    private static int numRowsPerFile;
    private static String warehouseLocation;
    private static String s3Secret;
    private static String s3KeyId;
    private static String s3Region;
    private static S3Client s3;
    private static String ravenAddress;
    private static RandomOpGenerator randomOpGenerator;
    private static final Schema SCHEMA = TPCDSSchema.STORE_SALES;

    private static final List<RavenCatalog> RAVEN_CATALOGS = Lists.newArrayList();
    private static final List<DuckDBConnection> DUCK_DB_CONNECTIONS = Lists.newArrayList();
    private static final List<List<Integer>> TABLE_VIDS = Lists.newArrayList();

    // data structures for measurements
    private static final List<List<TimePair>> LOAD_TABLE_TIMES = Lists.newArrayList();
    private static final List<List<TimePair>> INSERT_FILE_TIMES = Lists.newArrayList();
    private static final List<List<TimePair>> COMMIT_TIMES = Lists.newArrayList();
    private static final List<List<Boolean>> OP_SUCCESS = Lists.newArrayList();
    private static final List<List<String>> OP_TYPES = Lists.newArrayList();

    public static Map<String, String> parseJsonToMap(String jsonFilePath) throws IOException {
        File file = new File(jsonFilePath);

        // TypeReference is essential to tell Jackson the specific
        // Map implementation and generic types to use.
        return JSON_MAPPER.readValue(file, new TypeReference<Map<String, String>>() {});
    }

    public static void initCatalog() throws Exception {
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

    }

    public static void main(String[] args) throws Exception {
        // System.setProperty("datanucleus.plugin.pluginRegistryBundleCheck", "LOG");
        // read the config file for experimentation
        Map<String, String> expConfigs = parseJsonToMap(args[0]);
        refreshRatio = Float.parseFloat(expConfigs.get("refresh_ratio"));
        insertRatio = Float.parseFloat(expConfigs.get("insert_ratio"));
        durationStr = expConfigs.get("duration");
        workspaceName = expConfigs.get("workspace_name");
        dbName = expConfigs.get("db_name");
        tableName = expConfigs.get("table_name");
        numThreads = Integer.parseInt(expConfigs.get("num_threads"));
        txnPerCompaction = Integer.parseInt(expConfigs.get("txn_per_compaction"));
        numRowsPerFile = Integer.parseInt(expConfigs.get("num_rows_per_file"));
        warehouseLocation = expConfigs.get("warehouse_location");
        s3Secret = expConfigs.get("s3_secret");
        s3KeyId = expConfigs.get("s3_key_id");
        s3Region = expConfigs.get("s3_region");
        ravenAddress = expConfigs.get("raven_address");
        randomOpGenerator = new RandomOpGenerator(refreshRatio, insertRatio);

        initCatalog();
        for (int i = 0; i < numThreads; i++) {
            DUCK_DB_CONNECTIONS.add((DuckDBConnection) DriverManager.getConnection("jdbc:duckdb:"));
            try (Statement stmt = DUCK_DB_CONNECTIONS.get(i).createStatement()) {
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
        }

        int numRaven = Math.min(numThreads, 4);
        for (int i = 0; i < numRaven; i++) {
            RAVEN_CATALOGS.add(new RavenCatalog(ravenAddress));
        }

        for (int i = 0; i < numThreads; i++) {
            List<Integer> tableVids = Lists.newArrayList(NUM_TABLES + 1);
            TABLE_VIDS.add(tableVids);
            for (int j = 0; j < NUM_TABLES + 1; j++) {
                tableVids.add(0);
            }

            List<TimePair> loadTableTimes = Lists.newArrayList();
            LOAD_TABLE_TIMES.add(loadTableTimes);
            List<TimePair> insertFileTimes = Lists.newArrayList();
            INSERT_FILE_TIMES.add(insertFileTimes);
            List<TimePair> commitTimes = Lists.newArrayList();
            COMMIT_TIMES.add(commitTimes);
            List<Boolean> opSuccess = Lists.newArrayList();
            OP_SUCCESS.add(opSuccess);
            List<String> opTypes = Lists.newArrayList();
            OP_TYPES.add(opTypes);

        }

        runRavenExp(expConfigs);

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

    private static void runRavenExp(Map<String, String> expConfigs) {
        LocalTime duration = LocalTime.parse(durationStr);
        runRavenExpImpl(duration);

        String expResultDir = expConfigs.get("exp_result_dir");
        String workloadType;
        if (insertRatio - refreshRatio > 0.1) {
            workloadType = "write";
        }
        else if (insertRatio - refreshRatio < -0.1) {
            workloadType = "read";
        }
        else {
            workloadType = "mid";
        }

        String logFileName = String.format(Locale.getDefault(),"%s/gc1-iceberg-%s-%d-log.json",
                expResultDir, workloadType, numThreads);
        String summaryFileName = String.format("%s/summary.json", expResultDir);
        MetricsExporter.exportMetricsToLog(logFileName);
        MetricsExporter.appendSummaryToJson(summaryFileName);
    }

    private static void runRavenExpImpl(LocalTime duration) {
        runTaskForDuration((running, threadNum) -> {
            DuckDBConnection duckDbConn = DUCK_DB_CONNECTIONS.get(threadNum);
            RavenCatalog ravenCatalog = RAVEN_CATALOGS.get(threadNum % 4);
            List<Integer> tableVids = TABLE_VIDS.get(threadNum);
            int tablesPerThread = NUM_TABLES / numThreads;
            int minTableNum = threadNum * tablesPerThread + 1;
            int maxTableNum = (threadNum + 1) * tablesPerThread + 1;

            // Keep running until flag change
            while (running.get()) {
                int tableNum = ThreadLocalRandom.current().nextInt(minTableNum, maxTableNum);
                int opNum = randomOpGenerator.next();
                switch (opNum) {
                    case 0:
                        refreshOp(duckDbConn, ravenCatalog, tableVids, threadNum, tableNum);
                        break;
                    case 1:
                        // executes insert or (compact + expire snapshot)
                        writeOp(duckDbConn, ravenCatalog, threadNum, tableNum);
                        break;
                    default:
                        break;
                }
            }
        }, duration.toSecondOfDay(), TimeUnit.SECONDS);

    }

    private static void refreshOp(DuckDBConnection duckDbConn, RavenCatalog ravenCatalog, List<Integer> tableVids,
                                  Integer threadNum, int tableNum) {

        try (Statement stmt = duckDbConn.createStatement()) {
            Instant beforeLoadTables = Instant.now();

            String targetTableName = tableName + String.valueOf(tableNum);

            TableObject tableObject = ravenCatalog.loadTable(workspaceName, dbName, targetTableName);

            // retrieve all the file paths from raven which is delta from the current table snapshot.
            String query = String.format(Locale.getDefault(),
                    "SELECT file_path FROM FILELIST DELTA TableSnapshot(%d, %d, %d)",
                    tableObject.getSnapshotObjId(), tableVids.get(tableNum), tableObject.getSnapshotVid());
            byte[] resultSet = ravenCatalog.execQuery(query);
            // extract the data files from the result set buffer
            List<String> filesToScan = Lists.newArrayList();
            RavenCatalog.BufIterator bufIter = new RavenCatalog.BufIterator(resultSet);
            while (bufIter.valid()) {
                bufIter.next();
                String path = new String(resultSet, bufIter.dataIdx(), bufIter.elemSize(), UTF_8);
                bufIter.next();
                filesToScan.add(path);
            }

            tableVids.set(tableNum, tableObject.getSnapshotVid());

            Instant afterLoadTables = Instant.now();
            Instant afterInsertFile = afterLoadTables;

            if (!filesToScan.isEmpty()) {
                String fileToScanStr = filesToScan.stream()
                        .map(s -> "'" + s + "'")
                        .collect(Collectors.joining(","));
                stmt.execute(
                        String.format(
                                Locale.getDefault(),
                                "SELECT * FROM read_parquet([ %s ]);",
                                fileToScanStr));
                afterInsertFile = Instant.now();
            }

            Instant afterCommit = afterInsertFile;

            // record metrics
            LOAD_TABLE_TIMES.get(threadNum).add(new TimePair(beforeLoadTables, afterLoadTables));
            INSERT_FILE_TIMES.get(threadNum).add(new TimePair(afterLoadTables, afterInsertFile));
            COMMIT_TIMES.get(threadNum).add(new TimePair(afterInsertFile, afterCommit));
            OP_SUCCESS.get(threadNum).add(true);
            OP_TYPES.get(threadNum).add("refresh");

        } catch (SQLException e) {
            LOG.info("Database error occurred", e);
        }
    }

    private static void writeOp(DuckDBConnection duckDbConn, RavenCatalog ravenCatalog, Integer threadNum,
                                int tableNum) {
        try (Statement stmt = duckDbConn.createStatement()) {

            Instant beforeLoadTables = Instant.now();

            String targetTableName = tableName + String.valueOf(tableNum);

            TableObject tableObject = ravenCatalog.loadTable(workspaceName, dbName, targetTableName);

            Instant afterLoadTable = Instant.now();
            Instant afterInsertFile = null;
            Instant afterCommit = null;
            boolean success = false;

            String targetList = schemaToTargetList(SCHEMA);
            // first time insert a file since there is nothing to compact
            String opType = "insert";
            if (tableObject.getSnapshotVid() == 0 || tableObject.getSnapshotVid() % txnPerCompaction != 0) {
                // generate data
                stmt.execute(
                        String.format(
                                Locale.getDefault(),
                                "CREATE TEMP TABLE staging_data%d AS SELECT %s FROM generate_series(1, %d) AS t(x);",
                                threadNum,
                                targetList,
                                numRowsPerFile));
                String filePath = String.format("%s/%s.db/%s/%s.parquet", warehouseLocation, dbName, targetTableName,
                        UUID.randomUUID());

                stmt.execute(
                        String.format(
                                Locale.getDefault(), "COPY staging_data%d TO '%s' (FORMAT PARQUET);",
                                threadNum, filePath));
                stmt.execute(String.format(Locale.getDefault(), "DROP TABLE staging_data%d;", threadNum));

                afterInsertFile = Instant.now();

                // append files to Raven Catalog
                List<FileObject> newDataFiles = Lists.newArrayList();
                FileObject newFileObject = FileObject.newBuilder()
                        .setTag("newdata").setPath(filePath).build();
                newDataFiles.add(newFileObject);

                success = ravenCatalog.finalAppendFiles(tableObject, newDataFiles);

                if (!success) {
                    List<String> filePaths = Lists.newArrayList();
                    filePaths.add(filePath);
                    deleteFiles(filePaths);
                }

                afterCommit = Instant.now();
            }
            // compact files & expire snapshots
            else {
                opType = "compact";
                String query = String.format(Locale.getDefault(),
                        "SELECT file_path FROM FILELIST SNAPSHOT TableSnapshot(%d, %d) WHERE tag = 'newdata'",
                        tableObject.getSnapshotObjId(), tableObject.getSnapshotVid());
                byte[] resultSet = ravenCatalog.execQuery(query);
                // extract the data files from the result set buffer
                List<String> filesToReplace = Lists.newArrayList();
                RavenCatalog.BufIterator bufIter = new RavenCatalog.BufIterator(resultSet);
                while (bufIter.valid()) {
                    String path = new String(resultSet, bufIter.dataIdx(), bufIter.elemSize(), UTF_8);
                    bufIter.next();
                    filesToReplace.add(path);
                }

                afterLoadTable = Instant.now();

                if (filesToReplace.size() < 4) {
                    return;
                }

                String fileToScanStr = filesToReplace.stream()
                        .map(s -> "'" + s + "'")
                        .collect(Collectors.joining(","));

                // generate data
                stmt.execute(
                        String.format(
                                Locale.getDefault(),
                                "CREATE TEMP TABLE staging_data%d AS SELECT * FROM read_parquet([ %s ]);",
                                threadNum,
                                fileToScanStr));
                String filePath = String.format("%s/%s.db/%s/%s.parquet", warehouseLocation, dbName, targetTableName,
                        UUID.randomUUID());

                stmt.execute(
                        String.format(
                                Locale.getDefault(), "COPY staging_data%d TO '%s' (FORMAT PARQUET);",
                                threadNum, filePath));
                stmt.execute(String.format(Locale.getDefault(), "DROP TABLE staging_data%d;", threadNum));

                List<String> filePaths = Lists.newArrayList();
                filePaths.add(filePath);

                afterInsertFile = Instant.now();

                // add files to Raven Catalog
                List<FileObject> newDataFiles = Lists.newArrayList();
                FileObject newFileObject = FileObject.newBuilder()
                        .setTag("data").setPath(filePath).build();
                newDataFiles.add(newFileObject);

                // compact / merge the files
                success = ravenCatalog.finalRewriteFiles(tableObject, filesToReplace, newDataFiles);
                if (!success) {
                    deleteFiles(filePaths);
                }
                // expire snapshots
                success = success && ravenCatalog.expireSnapshotsRetainLast(workspaceName, dbName,
                        targetTableName, txnPerCompaction);

                afterCommit = Instant.now();
            }

            // record metrics
            LOAD_TABLE_TIMES.get(threadNum).add(new TimePair(beforeLoadTables, afterLoadTable));
            INSERT_FILE_TIMES.get(threadNum).add(new TimePair(afterLoadTable, afterInsertFile));
            COMMIT_TIMES.get(threadNum).add(new TimePair(afterInsertFile, afterCommit));
            OP_SUCCESS.get(threadNum).add(success);
            OP_TYPES.get(threadNum).add(opType);

        } catch (SQLException e) {
            LOG.info("Database error occurred", e);
        }
    }

    public static void runTaskForDuration(BiConsumer<AtomicBoolean, Integer> task, long duration, TimeUnit unit) {
        AtomicBoolean running = new AtomicBoolean(true); // The "Green Light"
        ExecutorService executor = Executors.newFixedThreadPool(numThreads);
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

        // Submit the task, passing the control flag 'running' and thread id to it
        for (int i = 0; i < numThreads; i++) {
            Integer threadId = i;
            @SuppressWarnings("unused")
            Future<?> future = executor.submit(() -> task.accept(running, threadId));
        }

        // Schedule the "Stop Signal"
        @SuppressWarnings("unused")
        Future<?> future2 = scheduler.schedule(() -> {
            running.set(false); // Flip the switch to Red
            scheduler.shutdown();
        }, duration, unit);

        // Shutdown executor safely
        executor.shutdown();
        try {
            // Wait for the task to finish its LAST iteration naturally.
            // We add a buffer (e.g., duration * 2) to ensure we don't kill it mid-process.
            // Since you prefer accuracy trade-offs over exceptions, we wait longer.
            if (!executor.awaitTermination(duration + 15, unit)) {
                executor.shutdownNow(); // Force kill only if it's genuinely stuck forever
            }
        } catch (InterruptedException e) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    public static void deleteFiles(List<String> s3Paths) {
        if (s3Paths == null || s3Paths.isEmpty()) {
            return;
        }

        // 1. Extract the single bucket name from the first path
        String bucket = URI.create(s3Paths.get(0)).getHost();

        // 2. Parse all paths into S3 Object Identifiers
        List<ObjectIdentifier> allKeys = new ArrayList<>(s3Paths.size());
        for (String path : s3Paths) {
            // URI path includes a leading slash (e.g., "/data/file.parquet").
            // S3 keys do not start with a slash, so we substring(1).
            String key = URI.create(path).getPath().substring(1);
            allKeys.add(ObjectIdentifier.builder().key(key).build());
        }

        // 3. Chunk and delete (AWS strict limit: 1000 keys per request)
        int chunkSize = 1000;
        for (int i = 0; i < allKeys.size(); i += chunkSize) {
            List<ObjectIdentifier> chunk = allKeys.subList(i, Math.min(i + chunkSize, allKeys.size()));

            Delete delete = Delete.builder()
                    .objects(chunk)
                    .quiet(true) // Recommended: Only returns a payload for files that failed to delete
                    .build();

            DeleteObjectsRequest request = DeleteObjectsRequest.builder()
                    .bucket(bucket)
                    .delete(delete)
                    .build();

            try {
                s3.deleteObjects(request);
//                // If quiet(true) is set, a successful deletion returns an empty error list
//                if (response.hasErrors() && !response.errors().isEmpty()) {
//                    for (S3Error error : response.errors()) {
//                        System.err.println("Failed to delete " + error.key() + ": " + error.message());
//                    }
//                } else {
//                    System.out.println("Successfully deleted batch of " + chunk.size() + " files.");
//                }
            } catch (S3Exception e) {
                System.err.println("AWS API error during batch delete: " + e.awsErrorDetails().errorMessage());
            }
        }
    }

}
