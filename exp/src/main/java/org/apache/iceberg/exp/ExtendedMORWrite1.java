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
import java.time.LocalTime;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
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

public class ExtendedMORWrite1 {
  // 1. Add this private constructor
  private ExtendedMORWrite1() {}

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

          if (i < COMPACT_TIMES.size()) {
            addTimeBlock(mapper, row, "compact", COMPACT_TIMES.get(i));
          }

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

      // 1. Calculate num_txn
      int numTxn = LOAD_TABLE_TIMES.size();

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
        Instant end;
        if (i < COMPACT_TIMES.size()) {
          end = COMPACT_TIMES.get(i).end;
        } else {
          // Fallback if compaction data is missing for this index
          end = COMMIT_TIMES.get(i).end;
        }

        // Calculate duration in milliseconds
        long duration = Duration.between(start, end).toMillis();
        totalLatencyMillis += duration;
      }

      double avgLatency = (numTxn == 0) ? 0 : (totalLatencyMillis / numTxn);

      // 4. Construct the JSON Object
      ObjectNode summaryNode = mapper.createObjectNode();
      summaryNode.put("exp_name", "ExtendedMORWrite1");
      summaryNode.put("exp_type", expType);
      summaryNode.put("txn_per_compaction", txnPerCompaction);
      summaryNode.put("duration", durationStr);
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

  private static final Logger LOG = LoggerFactory.getLogger(ExtendedMORWrite1.class);
  private static final ObjectMapper JSON_MAPPER = new ObjectMapper();
  private static String expType;
  private static String durationStr;
  private static String workspaceName;
  private static String dbName;
  private static String tableName;
  private static int numRowsPerFile;
  private static int txnPerCompaction;
  private static String warehouseLocation;
  private static String s3Secret;
  private static String s3KeyId;
  private static String s3Region;
  private static S3Client s3;
  private static String ravenAddress;
  private static final Schema SCHEMA = TPCDSSchema.STORE_SALES;

  // hive catalog
  private static HiveCatalog catalog;
  private static HiveCatalog2 catalog2;
  private static RavenCatalog ravenCatalog;
  private static DuckDBConnection duckDbConn;

  // data structures for measurements
  private static final List<TimePair> LOAD_TABLE_TIMES = Lists.newArrayList();
  private static final List<TimePair> INSERT_FILE_TIMES = Lists.newArrayList();
  private static final List<TimePair> COMMIT_TIMES = Lists.newArrayList();
  private static final List<TimePair> COMPACT_TIMES = Lists.newArrayList();
  private static final List<List<File2>> ADDED_FILES = Lists.newArrayList();
  // thread pool for s3 operations
  private static ExecutorService s3Executors;

  public static Map<String, String> parseJsonToMap(String jsonFilePath) throws IOException {
    File file = new File(jsonFilePath);

    // TypeReference is essential to tell Jackson the specific
    // Map implementation and generic types to use.
    return JSON_MAPPER.readValue(file, new TypeReference<Map<String, String>>() {});
  }

  public static void initCatalog() throws Exception {
//    Map<String, String> hive_conf = Maps.newHashMap();
//    hive_conf.put("fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
//    hive_conf.put("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
//    hive_conf.put("hive.metastore.warehouse.dir", warehouseLocation);
//    hive_conf.put("fs.s3a.access.key", s3KeyId);
//    hive_conf.put("fs.s3a.secret.key", s3Secret);
//    hive_conf.put("fs.s3a.endpoint.region", s3Region);

    // load the hive config
    HiveConf hiveConf = new HiveConf();
    // create new database
//    HiveMetaStoreClient metastoreClient = new HiveMetaStoreClient(hiveConf);
//    String dbPath = String.format("%s/%s.db", warehouseLocation, dbName);
//    Database db = new Database(dbName, "description", dbPath, Maps.newHashMap());
//    metastoreClient.createDatabase(db);
//    metastoreClient.close();

//    hiveMetastoreExtension = ExpCatalogExtension.builder().withWarehouse(warehouseLocation).withDatabase(dbName).withConfig(hive_conf).build();
//    hiveMetastoreExtension.beforeAll();
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
    durationStr = expConfigs.get("duration");
    workspaceName = expConfigs.get("workspace_name");
    dbName = expConfigs.get("db_name");
    tableName = expConfigs.get("table_name");
    numRowsPerFile = Integer.parseInt(expConfigs.get("num_rows_per_file"));
    warehouseLocation = expConfigs.get("warehouse_location");
    txnPerCompaction = Integer.parseInt(expConfigs.get("txn_per_compaction"));
    s3Secret = expConfigs.get("s3_secret");
    s3KeyId = expConfigs.get("s3_key_id");
    s3Region = expConfigs.get("s3_region");
    ravenAddress = expConfigs.get("raven_address");

    initCatalog();
    duckDbConn = (DuckDBConnection) DriverManager.getConnection("jdbc:duckdb:");
    s3Executors = Executors.newFixedThreadPool(txnPerCompaction + 1);

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

    s3Executors.shutdown();
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

  private static Metrics computeStats(Schema schema, int i) {
    Map<Integer, Long> valueCounts = Maps.newHashMap();
    Map<Integer, Long> nullValueCounts = Maps.newHashMap();
    Map<Integer, Long> nanValueCounts = Maps.newHashMap();
    Map<Integer, ByteBuffer> lowerBounds = Maps.newHashMap();
    Map<Integer, ByteBuffer> upperBounds = Maps.newHashMap();

    ByteBuffer lowerBoundInt = ByteBuffer.allocate(Integer.BYTES);
    lowerBoundInt.order(ByteOrder.LITTLE_ENDIAN); // Set byte order
    lowerBoundInt.putInt(i * numRowsPerFile + 1);
    lowerBoundInt.rewind();

    ByteBuffer upperBoundInt = ByteBuffer.allocate(Integer.BYTES);
    upperBoundInt.order(ByteOrder.LITTLE_ENDIAN); // Set byte order
    upperBoundInt.putInt((i + 1) * numRowsPerFile);
    upperBoundInt.rewind();

    // There is a bit of bug for string field, but the store_sales does not contain any string fields
    ByteBuffer lowerBoundStr = ByteBuffer.wrap(String.valueOf(i * numRowsPerFile + 1).getBytes(StandardCharsets.UTF_8));
    ByteBuffer upperBoundStr = ByteBuffer.wrap(String.valueOf((i + 1) * numRowsPerFile).getBytes(StandardCharsets.UTF_8));

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
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).build();
    TableIdentifier tableIdent = TableIdentifier.of(dbName, tableName);
    String location = String.format("%s/%s.db/%s", warehouseLocation, dbName, tableName);
    catalog2.createNamespace(Namespace.of(dbName));
    catalog2.createTable(tableIdent, SCHEMA, spec, location, ImmutableMap.of());

    ravenCatalog = new RavenCatalog(ravenAddress);
    LocalTime duration = LocalTime.parse(durationStr);
    runRavenExpImpl(duration);

    String expResultDir = expConfigs.get("exp_result_dir");
    String logFileName = String.format(Locale.getDefault(),"%s/extendedmorwrite1-iceberg-raven-%d-%d-log.json",
            expResultDir, txnPerCompaction, numRowsPerFile);
    String summaryFileName = String.format("%s/summary.json", expResultDir);
    MetricsExporter.exportMetricsToLog(logFileName);
    MetricsExporter.appendSummaryToJson(summaryFileName);
  }

  private static void runVanillaExp(Map<String, String> expConfigs) {
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).build();
    TableIdentifier tableIdent = TableIdentifier.of(dbName, tableName);
    String location = String.format("%s/%s.db/%s", warehouseLocation, dbName, tableName);
    catalog.createNamespace(Namespace.of(dbName));
    catalog.createTable(tableIdent, SCHEMA, spec, location, ImmutableMap.of());

    LocalTime duration = LocalTime.parse(durationStr);
    runVanillaExpImpl(duration);

    String expResultDir = expConfigs.get("exp_result_dir");
    String logFileName = String.format(Locale.getDefault(),"%s/extendedmorwrite1-iceberg-vanilla-%d-%d-log.json",
            expResultDir, txnPerCompaction, numRowsPerFile);
    String summaryFileName = String.format("%s/summary.json", expResultDir);
    MetricsExporter.exportMetricsToLog(logFileName);
    MetricsExporter.appendSummaryToJson(summaryFileName);
  }

  private static void runRavenExpImpl(LocalTime duration) {
    runTaskForDuration(running -> {
      String targetList = schemaToTargetList(SCHEMA);
      PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).build();

      Table table = catalog2.loadTable(TableIdentifier.of(dbName, tableName));
      // Keep running until flag change
      int i = 0;
      while (running.get()) {
        try (Statement stmt = duckDbConn.createStatement()) {
          List<File2> fileLogs = Lists.newArrayList();
          Instant beforeLoadTable = Instant.now();

          // Load table. Load from both hive and raven
          TableObject tableObject = ravenCatalog.loadTable(workspaceName, dbName, tableName);
          Instant afterLoadTable = Instant.now();

          // generate data
          stmt.execute(
                  String.format(
                          Locale.getDefault(),
                          "CREATE TEMP TABLE staging_data AS SELECT %s FROM generate_series(%d, %d) AS t(x);",
                          targetList,
                          i * numRowsPerFile + 1,
                          (i + 1) * numRowsPerFile));
          i += 1;
          // write the data to S3 as a parquet file
          String filePath = String.format("%s/%s.parquet", table.location(), UUID.randomUUID());
          stmt.execute(
                  String.format(
                          Locale.getDefault(), "COPY staging_data TO '%s' (FORMAT PARQUET);", filePath));
          stmt.execute("DROP TABLE staging_data;");

          long fileSize = getFileSize(filePath);

          Instant afterInsertFile = Instant.now();

          FileObject newFileObject = FileObject.newBuilder().setPath(filePath).setSize((int) fileSize)
                  .setFormat("parquet").setTag("newdata").build();
          List<FileObject> newFilesList = Lists.newArrayList();
          newFilesList.add(newFileObject);

          ravenCatalog.finalAppendFiles(tableObject, newFilesList);

          Instant afterCommit = Instant.now();
          Instant afterCompact = afterCommit;

          // perform compaction
          // 1. Get the newdata files, using ExecQuery
          // 2. Read the newdata files, collecting statistics information, using DuckDB.
          // 3. Commit the newdata files to Iceberg and get the metadata file, manifestlist file, and manifest file.
          // 4. Replace the newdata files with different tags, Replace the metadata file, manifestlist file, and manifest file.
          if (tableObject.getSnapshotVid() % txnPerCompaction == 0) {
            tableObject = ravenCatalog.loadTable(workspaceName, dbName, tableName);
            String query = String.format(Locale.getDefault(),
                    "SELECT file_path, file_size, format FROM FILELIST SNAPSHOT TableSnapshot(%d, %d) WHERE tag = 'newdata'",
                    tableObject.getSnapshotObjId(), tableObject.getSnapshotVid());

            byte[] resultSet = ravenCatalog.execQuery(query);
            // extract the newdata files from the result set buffer
            List<FileObject> newDataFiles = Lists.newArrayList();
            RavenCatalog.BufIterator bufIter = new RavenCatalog.BufIterator(resultSet);
            while (bufIter.valid()) {
              String path = new String(resultSet, bufIter.dataIdx(), bufIter.elemSize(), UTF_8);
              bufIter.next();
              String size = new String(resultSet, bufIter.dataIdx(), bufIter.elemSize(), UTF_8);
              bufIter.next();
              String format = new String(resultSet, bufIter.dataIdx(), bufIter.elemSize(), UTF_8);
              bufIter.next();
              // change tag to data
              FileObject file = FileObject.newBuilder().setPath(path).setSize(Integer.parseInt(size))
                      .setFormat(format).setTag("data").build();
              newDataFiles.add(file);
            }

            // start Iceberg transaction
            AppendFiles txn = table.newFastAppend();

            // extract the statistics information from the parquet files
            List<Callable<Metrics>> s3Tasks = Lists.newArrayList();
            for (FileObject file : newDataFiles) {
              s3Tasks.add(() -> S3ParquetStats.extractStats(s3, file.getPath()));
            }
            // stats extraction is performed in parallel for performance
            try {
              List<Future<Metrics>> stats = s3Executors.invokeAll(s3Tasks);
              for (int j = 0; j < newDataFiles.size(); j++) {
                DataFile newDataFile =
                        DataFiles.builder(spec)
                                .withFileSizeInBytes(newDataFiles.get(j).getSize())
                                .withFormat(FileFormat.PARQUET)
                                .withMetrics(stats.get(j).get())
                                .withPath(newDataFiles.get(j).getPath())
                                .build();
                txn.appendFile(newDataFile);
              }
            }
            catch (InterruptedException | ExecutionException e) {
              LOG.info("InterruptedException or ExecutionException", e);
            }

            // commit to Iceberg
            txn.commit2(fileLogs);

            // 4. Replace the newdata files with different tags, Replace the metadata file, manifestlist file, and manifest file.
            List<String> filesToReplace = Lists.newArrayList();

            // newdata files to replace (changing the tags to 'data')
            for (FileObject file : newDataFiles) {
              filesToReplace.add(file.getPath());
            }

            // metadata files (metadata, manifestlist, manifest) to replace & add
            for (File2 file : fileLogs) {
              switch (file.fileType()) {
                case ADD:
                  FileObject newMetadataFileObject = FileObject.newBuilder()
                          .setTag(file.tag()).setPath(file.path()).build();
                  // reusing newDataFiles list to hold all new files to add
                  newDataFiles.add(newMetadataFileObject);
                  break;
                case DELETE:
                  filesToReplace.add(file.path());
                  break;
                default:
                  break;
              }
            }

            ravenCatalog.finalRewriteFiles(tableObject, filesToReplace, newDataFiles);

            afterCompact = Instant.now();
          }

          LOAD_TABLE_TIMES.add(new TimePair(beforeLoadTable, afterLoadTable));
          INSERT_FILE_TIMES.add(new TimePair(afterLoadTable, afterInsertFile));
          COMMIT_TIMES.add(new TimePair(afterInsertFile, afterCommit));
          COMPACT_TIMES.add(new TimePair(afterCommit, afterCompact));
          fileLogs.add(new File2(filePath, File2.File2Type.ADD, "data"));
          ADDED_FILES.add(fileLogs);
        } catch (SQLException e) {
          LOG.info("Database error occurred", e);
        }
      }
    }, duration.toSecondOfDay(), TimeUnit.SECONDS);

  }

  private static void runVanillaExpImpl(LocalTime duration) {
    runTaskForDuration(running -> {
      String targetList = schemaToTargetList(SCHEMA);
      PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).build();

      // Keep running until flag change
      int i = 0;
      while (running.get()) {
        try (Statement stmt = duckDbConn.createStatement()) {
          List<File2> fileLogs = Lists.newArrayList();

          Instant beforeLoadTable = Instant.now();

          // load table & start transaction
          Table table = catalog.loadTable(TableIdentifier.of(dbName, tableName));
          AppendFiles txn = table.newFastAppend();

          Instant afterLoadTable = Instant.now();

          // generate data
          stmt.execute(
                  String.format(
                          Locale.getDefault(),
                          "CREATE TEMP TABLE staging_data AS SELECT %s FROM generate_series(%d, %d) AS t(x);",
                          targetList,
                          i * numRowsPerFile + 1,
                          (i + 1) * numRowsPerFile));

          // construct file statistics
          Metrics metrics = computeStats(SCHEMA, i);
          i += 1;

          // write the data to S3 as a parquet file
          String filePath = String.format("%s/%s.parquet", table.location(), UUID.randomUUID());
          stmt.execute(
                  String.format(
                          Locale.getDefault(), "COPY staging_data TO '%s' (FORMAT PARQUET);", filePath));
          stmt.execute("DROP TABLE staging_data;");

          long fileSize = getFileSize(filePath);

          Instant afterInsertFile = Instant.now();

          DataFile newFile =
                  DataFiles.builder(spec)
                          .withFileSizeInBytes(fileSize)
                          .withFormat(FileFormat.PARQUET)
                          .withMetrics(metrics)
                          .withPath(filePath)
                          .build();

          txn.appendFile(newFile);
          txn.commit2(fileLogs);

          Instant afterCommit = Instant.now();

          LOAD_TABLE_TIMES.add(new TimePair(beforeLoadTable, afterLoadTable));
          INSERT_FILE_TIMES.add(new TimePair(afterLoadTable, afterInsertFile));
          COMMIT_TIMES.add(new TimePair(afterInsertFile, afterCommit));
          // also add the new data file
          fileLogs.add(new File2(filePath, File2.File2Type.ADD, "data"));
          ADDED_FILES.add(fileLogs);
        } catch (SQLException e) {
          LOG.info("Database error occurred", e);
        }
      }
    }, duration.toSecondOfDay(), TimeUnit.SECONDS);

  }

  public static void runTaskForDuration(Consumer<AtomicBoolean> task, long duration, TimeUnit unit) {
    AtomicBoolean running = new AtomicBoolean(true); // The "Green Light"
    ExecutorService executor = Executors.newSingleThreadExecutor();
    ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

    // Submit the task, passing the control flag 'running' to it
    @SuppressWarnings("unused")
    Future<?> future = executor.submit(() -> task.accept(running));

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
      if (!executor.awaitTermination(duration + 5, unit)) {
        executor.shutdownNow(); // Force kill only if it's genuinely stuck forever
      }
    } catch (InterruptedException e) {
      executor.shutdownNow();
      Thread.currentThread().interrupt();
    }
  }


  private static long getFileSize(String path) {
    URI uri = URI.create(path);
    String bucket = uri.getHost();
    String key = uri.getPath().substring(1);
    HeadObjectRequest headRequest = HeadObjectRequest.builder().bucket(bucket).key(key).build();
    return s3.headObject(headRequest).contentLength();
  }

  //    public class DuckDBFastGenerate {
  //        public static void main(String[] args) {
  //            // Use in-memory DuckDB. The data will stream to S3, not store locally.
  //            String jdbcUrl = "jdbc:duckdb:";
  //
  //            try (Connection conn = DriverManager.getConnection(jdbcUrl);
  //                 Statement stmt = conn.createStatement()) {
  //
  //                // 1. Install and Load required extensions
  //                stmt.execute("INSTALL httpfs; LOAD httpfs;");
  //                stmt.execute("INSTALL parquet; LOAD parquet;");
  //
  //                // 2. Configure S3 Credentials (Modern Syntax)
  //                // Replace with your actual credentials or use PROVIDER credential_chain
  //                String secretSql = """
  //                CREATE SECRET IF NOT EXISTS s3_secret (
  //                    TYPE S3,
  //                    KEY_ID 'YOUR_ACCESS_KEY',
  //                    SECRET 'YOUR_SECRET_KEY',
  //                    REGION 'us-east-1'
  //                );
  //            """;
  //                stmt.execute(secretSql);
  //
  //                // 3. The "Fast" Step: Generate & Write in one go
  //                // This generates 10,000 rows and streams directly to S3 as Parquet
  //                String copySql = """
  //                COPY (
  //                    SELECT
  //                        x::INTEGER AS id,
  //                        'row_' || x AS name,
  //                        random() AS value,
  //                        NOW() - (x || ' seconds')::INTERVAL AS timestamp
  //                    FROM generate_series(1, 10000) AS t(x)
  //                )
  //                TO 's3://your-bucket-name/output/data.parquet'
  //                (FORMAT PARQUET, COMPRESSION 'ZSTD');
  //            """;
  //
  //                long start = System.currentTimeMillis();
  //                stmt.execute(copySql);
  //                long end = System.currentTimeMillis();
  //
  //                System.out.println("Write complete in " + (end - start) + "ms");
  //
  //            } catch (Exception e) {
  //                e.printStackTrace();
  //            }
  //        }
  //    }

  //    try (Connection conn = DriverManager.getConnection("jdbc:duckdb:");
  //    Statement stmt = conn.createStatement()) {
  //
  //        // 1. Setup & Secrets
  //        stmt.execute("INSTALL httpfs; LOAD httpfs; INSTALL parquet; LOAD parquet;");
  //        stmt.execute("CREATE SECRET (TYPE S3, KEY_ID '...', SECRET '...', REGION '...');");
  //
  //        // 2. Generate to Memory (Zero Disk I/O)
  //        // using CREATE TEMP TABLE ensures it cleans up automatically on connection close
  //        stmt.execute("""
  //        CREATE TEMP TABLE staging_data AS
  //        SELECT
  //            x::INTEGER AS id,
  //            random() AS val
  //        FROM generate_series(1, 10000) AS t(x);
  //    """);
  //
  //        // 3. Collect Statistics (Instant RAM access)
  //        // You can now execute any SQL aggregation you want
  //        try (var rs = stmt.executeQuery("SELECT MIN(val), MAX(val), AVG(val) FROM
  // staging_data")) {
  //            if (rs.next()) {
  //                System.out.println("Min: " + rs.getDouble(1));
  //                System.out.println("Max: " + rs.getDouble(2));
  //            }
  //        }
  //
  //        // 4. Write to S3
  //        stmt.execute("COPY staging_data TO 's3://bucket/data.parquet' (FORMAT PARQUET);");
  //    }

}
