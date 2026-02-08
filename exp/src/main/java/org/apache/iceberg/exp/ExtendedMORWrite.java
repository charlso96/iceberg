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

import static org.apache.iceberg.types.Types.NestedField.required;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.time.LocalTime;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Database;
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
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Types;
import org.duckdb.DuckDBConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;

public class ExtendedMORWrite {
  // 1. Add this private constructor
  private ExtendedMORWrite() {}

  public static class TimePair {
    private final Instant start;
    private final Instant end;

    public TimePair(Instant start, Instant end) {
      this.start = start;
      this.end = end;
    }

    public long getDurationMillis() {
      return end.toEpochMilli() - start.toEpochMilli();
    }

    // Getters, equals(), hashCode(), and toString() would go here
  }

  private static final Logger LOG = LoggerFactory.getLogger(ExtendedMORWrite.class);
  private static final ObjectMapper JSON_MAPPER = new ObjectMapper();
//  private static ExpCatalogExtension hiveMetastoreExtension;
  private static String dbName;
  private static String tableName;
  private static int numRowsPerFile;
  //  private static int TXN_PER_COMPACTION;
  private static String warehouseLocation;
  private static String s3Secret;
  private static String s3KeyId;
  private static String s3Region;
  private static S3Client s3;
  static final Schema SCHEMA =
      new Schema(
          Types.StructType.of(
                  required(1, "ss_sold_time_sk", Types.IntegerType.get()),
                  required(2, "ss_item_sk", Types.IntegerType.get()),
                  required(3, "ss_customer_sk", Types.IntegerType.get()),
                  required(4, "ss_cdemo_sk", Types.IntegerType.get()),
                  required(5, "ss_hdemo_sk", Types.IntegerType.get()),
                  required(6, "ss_addr_sk", Types.IntegerType.get()),
                  required(7, "ss_store_sk", Types.IntegerType.get()),
                  required(8, "ss_promo_sk", Types.IntegerType.get()),
                  required(9, "ss_ticket_number", Types.IntegerType.get()),
                  required(10, "ss_wholesale_cost", Types.IntegerType.get()),
                  required(11, "ss_list_price", Types.DecimalType.of(11, 2)),
                  required(12, "ss_sales_price", Types.DecimalType.of(11, 2)),
                  required(13, "ss_ext_discount_amt", Types.DecimalType.of(11, 2)),
                  required(14, "ss_ext_sales_price", Types.DecimalType.of(11, 2)),
                  required(15, "ss_ext_wholesale_cost", Types.DecimalType.of(11, 2)),
                  required(16, "ss_ext_list_price", Types.DecimalType.of(11, 2)),
                  required(17, "ss_ext_tax", Types.DecimalType.of(11, 2)),
                  required(18, "ss_coupon_amt", Types.DecimalType.of(11, 2)),
                  required(19, "ss_net_paid", Types.DecimalType.of(11, 2)),
                  required(20, "ss_net_paid_inc_tax", Types.DecimalType.of(11, 2)),
                  required(21, "ss_net_profit", Types.DecimalType.of(11, 2)))
              .fields());

  // hive catalog
  private static HiveCatalog catalog;
  private static DuckDBConnection duckDbConn;

  // data structures for measurements
  private static final List<TimePair> LOAD_TABLE_TIMES = Lists.newArrayList();
  private static final List<TimePair> INSERT_FILE_TIMES = Lists.newArrayList();
  private static final List<TimePair> COMMIT_TIMES = Lists.newArrayList();
  //  private static final List<TimePair> COMPACT_TIMES = Lists.newArrayList();
  private static final List<List<File2>> ADDED_FILES = Lists.newArrayList();

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
    //        conf.put(S3FileIOProperties.SESSION_TOKEN, StaticClientFactory.class.getName());
    // for accessing s3 later on
    AwsClientFactories.defaultFactory().initialize(conf);
    s3 = AwsClientFactories.defaultFactory().s3();
    catalog =
        (HiveCatalog)
            CatalogUtil.loadCatalog(
                HiveCatalog.class.getName(),
                CatalogUtil.ICEBERG_CATALOG_TYPE_HIVE,
                conf, hiveConf);
  }

  public static void main(String[] args) throws Exception {
    // System.setProperty("datanucleus.plugin.pluginRegistryBundleCheck", "LOG");
    // read the config file for experimentation
    Map<String, String> expConfigs = parseJsonToMap(args[0]);
    dbName = expConfigs.get("db_name");
    tableName = expConfigs.get("table_name");
    numRowsPerFile = Integer.parseInt(expConfigs.get("num_rows_per_file"));
    warehouseLocation = expConfigs.get("warehouse_location");
    //    TXN_PER_COMPACTION = Integer.parseInt(expConfigs.get("txn_per_compaction"));
    s3Secret = expConfigs.get("s3_secret");
    s3KeyId = expConfigs.get("s3_key_id");
    s3Region = expConfigs.get("s3_region");

    initCatalog();

    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).build();
    TableIdentifier tableIdent = TableIdentifier.of(dbName, tableName);
    String location = String.format("%s/%s.db/%s", warehouseLocation, dbName, tableName);
    catalog.createNamespace(Namespace.of(dbName));
    catalog.createTable(tableIdent, SCHEMA, spec, location, ImmutableMap.of());
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

    if (expConfigs.get("exp_type").equals("raven")) {
      //      runRavenExp(expConfigs);
    } else {
      runVanillaExp(expConfigs);
    }

//    hiveMetastoreExtension.afterAll();
  }

  private static String schemaToTargetList(Schema schema) {
    List<Types.NestedField> columns = schema.columns();
    StringBuilder sb = new StringBuilder();
    for (Types.NestedField column : columns) {
      switch (column.type().typeId()) {
        case INTEGER:
          sb.append(String.format(Locale.getDefault(), "x::INTEGER AS %s,", column.name()));
          break;
        case DECIMAL:
          sb.append(String.format(Locale.getDefault(), "x::DECIMAL(11,2) AS %s,", column.name()));
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

  private static Metrics computeMetrics(Schema schema) {
    Map<Integer, Long> valueCounts = Maps.newHashMap();
    Map<Integer, Long> nullValueCounts = Maps.newHashMap();
    Map<Integer, Long> nanValueCounts = Maps.newHashMap();
    //
    List<Types.NestedField> columns = schema.columns();
    for (Types.NestedField column : columns) {
      valueCounts.put(column.fieldId(), (long) numRowsPerFile);
      nullValueCounts.put(column.fieldId(), 0L);
      nanValueCounts.put(column.fieldId(), 0L);
    }

    return new Metrics((long) numRowsPerFile, null, valueCounts, nullValueCounts, nanValueCounts);
  }

  //  private static void runRavenExp(Map<String, String> expConfigs) {}

  private static void runVanillaExp(Map<String, String> expConfigs) {
    LocalTime duration = LocalTime.parse(expConfigs.get("duration"));
    runTaskForDuration(
        ExtendedMORWrite::runVanillaExpImpl, duration.toSecondOfDay(), TimeUnit.SECONDS);

    // TODO figure out how to output the result as files
    //    String expLocation = expConfigs.get("exp_location");
    //    String logFileName = String.format("morwrite-iceberg-vanilla-%d-log.json",
    // numRowsPerFile);
    //    String summaryFileName =
    //        String.format("morwrite-iceberg-vanilla-%d-summary.json", numRowsPerFile);
    LOG.info("Length of list: {}", ADDED_FILES.size());
  }

  private static void runVanillaExpImpl() {
    String targetList = schemaToTargetList(SCHEMA);
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).build();

    // Keep running until interrupted
    while (!Thread.currentThread().isInterrupted()) {
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
                "CREATE TEMP TABLE staging_data AS SELECT %s FROM generate_series(1, %d) AS t(x);",
                targetList,
                numRowsPerFile));

        // construct file statistics
        Metrics metrics = computeMetrics(SCHEMA);

        // write the data to S3 as a parquet file
        String filePath = String.format("%s/%s", table.location(), UUID.randomUUID());
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
        ADDED_FILES.add(fileLogs);
      } catch (SQLException e) {
        LOG.info("Database error occurred", e);
      }
    }
  }

  public static void runTaskForDuration(Runnable task, long duration, TimeUnit unit) {
    ExecutorService executor = Executors.newSingleThreadExecutor();
    ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

    // Start the task
    Future<?> future = executor.submit(task);

    // Schedule the interruption
    @SuppressWarnings("unused")
    Future<?> future2 =
        scheduler.schedule(
            () -> {
              future.cancel(true); // 'true' allows interrupting the thread
              executor.shutdownNow();
              scheduler.shutdown();
            },
            duration,
            unit);

    // Keep main thread alive until scheduler finishes (optional)
    try {
      executor.awaitTermination(duration + 3, unit);
    } catch (InterruptedException e) {
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
