/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  *   http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing,
 *  * software distributed under the License is distributed on an
 *  * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  * KIND, either express or implied.  See the License for the
 *  * specific language governing permissions and limitations
 *  * under the License.
 *
 */

package org.apache.iceberg.exp;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
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
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hive.ExpCatalogExtension;
import org.apache.iceberg.hive.HiveCatalog;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Types;
import static org.apache.iceberg.types.Types.NestedField.required;
import org.duckdb.DuckDBConnection;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;

public class ExtendedMORWrite {
    public record TimePair(Instant start, Instant end) {
        public long getDurationMillis() {
            return end.toEpochMilli() - start.toEpochMilli();
        }
    }

    private static final ObjectMapper json_mapper = new ObjectMapper();
    private static ExpCatalogExtension HIVE_METASTORE_EXTENSION;
    private static String DB_NAME;
    private static String TABLE_NAME;
    private static int NUM_ROWS_PER_FILE;
    private static int TXN_PER_COMPACTION;
    private static String WAREHOUSE_LOCATION;
    public static String S3_SECRET;
    public static String S3_KEY_ID;
    public static String S3_REGION;
    private static S3Client S3;
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
                                    required(11, "ss_list_price", Types.DecimalType.of(11,2)),
                                    required(12, "ss_sales_price", Types.DecimalType.of(11,2)),
                                    required(13, "ss_ext_discount_amt", Types.DecimalType.of(11,2)),
                                    required(14, "ss_ext_sales_price", Types.DecimalType.of(11,2)),
                                    required(15, "ss_ext_wholesale_cost", Types.DecimalType.of(11,2)),
                                    required(16, "ss_ext_list_price", Types.DecimalType.of(11,2)),
                                    required(17, "ss_ext_tax", Types.DecimalType.of(11,2)),
                                    required(18, "ss_coupon_amt", Types.DecimalType.of(11,2)),
                                    required(19, "ss_net_paid", Types.DecimalType.of(11,2)),
                                    required(20, "ss_net_paid_inc_tax", Types.DecimalType.of(11,2)),
                                    required(20, "ss_net_profit", Types.DecimalType.of(11,2)))
                            .fields());

    // hive catalog
    private static HiveCatalog catalog;
    private static DuckDBConnection duck_db_conn;

    // data structures for measurements
    private static final List<TimePair> LOAD_TABLE_TIMES = new ArrayList<>();
    private static final List<TimePair> INSERT_FILE_TIMES = new ArrayList<>();
    private static final List<TimePair> COMMIT_TIMES = new ArrayList<>();
    private static final List<TimePair> COMPACT_TIMES = new ArrayList<>();
    private static final List<List<File2>> ADDED_FILES = new ArrayList<>();
    /**
     * Parses a JSON file into a Map with String keys and Object values.
     * * @param jsonFilePath Path to the source .json file
     * @return A Map representing the JSON structure
     * @throws IOException If the file is missing or JSON is malformed
     */
    public static Map<String, String> parseJsonToMap(String jsonFilePath) throws IOException {
        File file = new File(jsonFilePath);

        // TypeReference is essential to tell Jackson the specific
        // Map implementation and generic types to use.
        return json_mapper.readValue(file, new TypeReference<Map<String, String>>() {});
    }

    public static void initCatalog() {
        Map<String, String> conf = Maps.newHashMap();
        conf.put(CatalogProperties.URI, "thrift://localhost:9083");
        conf.put(CatalogProperties.CLIENT_POOL_CACHE_EVICTION_INTERVAL_MS,
                String.valueOf(TimeUnit.SECONDS.toMillis(10)));
        conf.put(CatalogProperties.WAREHOUSE_LOCATION, WAREHOUSE_LOCATION);
        conf.put(CatalogProperties.FILE_IO_IMPL, S3FileIO.class.getName());
        conf.put(S3FileIOProperties.ACCESS_KEY_ID, S3_KEY_ID);
        conf.put(S3FileIOProperties.SECRET_ACCESS_KEY, S3_SECRET);
        conf.put(AwsClientProperties.CLIENT_REGION, S3_REGION);
//        conf.put(S3FileIOProperties.SESSION_TOKEN, StaticClientFactory.class.getName());
        // for accessing s3 later on
        AwsClientFactories.defaultFactory().initialize(conf);
        S3 = AwsClientFactories.defaultFactory().s3();
        catalog =
                (HiveCatalog)
                        CatalogUtil.loadCatalog(
                                HiveCatalog.class.getName(),
                                CatalogUtil.ICEBERG_CATALOG_TYPE_HIVE,
                                conf,
                                HIVE_METASTORE_EXTENSION.hiveConf());
    }

    public static void main(String[] args) throws Exception {
        // read the config file for experimentation
        Map<String, String> exp_configs = parseJsonToMap(args[0]);
        DB_NAME = exp_configs.get("db_name");
        TABLE_NAME = exp_configs.get("table_name");
        NUM_ROWS_PER_FILE = Integer.parseInt(exp_configs.get("num_rows_per_file"));
        WAREHOUSE_LOCATION = exp_configs.get("warehouse_location");
        TXN_PER_COMPACTION = Integer.parseInt(exp_configs.get("txn_per_compaction"));
        S3_SECRET = exp_configs.get("s3_secret");
        S3_KEY_ID = exp_configs.get("s3_key_id");
        S3_REGION = exp_configs.get("s3_region");

        HIVE_METASTORE_EXTENSION = ExpCatalogExtension.builder().withDatabase(DB_NAME).build();
        HIVE_METASTORE_EXTENSION.beforeAll();
        initCatalog();

        PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).build();
        TableIdentifier tableIdent = TableIdentifier.of(DB_NAME, TABLE_NAME);
        String location = Path.of(WAREHOUSE_LOCATION).resolve(DB_NAME).resolve(TABLE_NAME).toString();

        Table table = catalog.createTable(tableIdent, SCHEMA, spec, location, ImmutableMap.of());
        duck_db_conn = (DuckDBConnection) DriverManager.getConnection("jdbc:duckdb:");
        try (Statement stmt = duck_db_conn.createStatement()) {
            stmt.execute("INSTALL httpfs; LOAD httpfs;");
            stmt.execute("INSTALL parquet; LOAD parquet;");
            String secretSql = """
                CREATE SECRET IF NOT EXISTS s3_secret (
                    TYPE S3,
                    KEY_ID '%s',
                    SECRET '%s',
                    REGION '%s'
                );""".formatted(S3_KEY_ID, S3_SECRET, S3_REGION);
            stmt.execute(secretSql);
        }

        if (exp_configs.get("exp_type").equals("raven")) {
            runRavenExp(exp_configs);
        }
        else {
            runVanillaExp(exp_configs);
        }

        HIVE_METASTORE_EXTENSION.afterAll();
    }

    private static String schemaToTargetList(Schema schema) {
        List<Types.NestedField> columns = schema.columns();
        StringBuilder sb = new StringBuilder();
        for (Types.NestedField column: columns) {
            switch(column.type().typeId()) {
                case INTEGER:
                    sb.append("x::INTEGER AS %s,".formatted(column.name()));
                    break;
                case DECIMAL:
                    sb.append("x::DECIMAL(11,2) AS %s,".formatted(column.name()));
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

    private static Metrics computeMetrics(Statement stmt, Schema schema) {
        Map<Integer, Long> valueCounts = Maps.newHashMap();
        Map<Integer, Long> nullValueCounts = Maps.newHashMap();
        Map<Integer, Long> nanValueCounts = Maps.newHashMap();
//
        List<Types.NestedField> columns = schema.columns();
        for (Types.NestedField column : columns) {
            valueCounts.put(column.fieldId(), (long) NUM_ROWS_PER_FILE);
            nullValueCounts.put(column.fieldId(), 0L);
            nanValueCounts.put(column.fieldId(), 0L);
        }

        return new Metrics((long) NUM_ROWS_PER_FILE, null, valueCounts, nullValueCounts, nanValueCounts);
    }

    private static void runRavenExp(Map<String, String> exp_configs) {

    }

    private static void runVanillaExp(Map<String, String> exp_configs) {
        LocalTime duration = LocalTime.parse(exp_configs.get("duration"));
        runTaskForDuration(ExtendedMORWrite::runVanillaExpImpl, duration.toSecondOfDay(), TimeUnit.SECONDS);

        String exp_location = exp_configs.get("exp_location");
        String log_file_name = "morwrite-iceberg-vanilla-%d-log.json".formatted(NUM_ROWS_PER_FILE);
        String summary_file_name = "morwrite-iceberg-vanilla-%d-summary.json".formatted(NUM_ROWS_PER_FILE);
        // TODO figure out how to output the result as files
        System.out.println("Length of list: " + ADDED_FILES.size());
    }

    private static void runVanillaExpImpl() {
        String targetList = schemaToTargetList(SCHEMA);
        PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).build();

        try {
            // Keep running until interrupted
            while (!Thread.currentThread().isInterrupted()) {
                try (Statement stmt = duck_db_conn.createStatement()) {
                    List<File2> fileLogs = new ArrayList<>();

                    Instant before_load_table = Instant.now();

                    // load table & start transaction
                    Table table = catalog.loadTable(TableIdentifier.of(DB_NAME, TABLE_NAME));
                    AppendFiles txn = table.newFastAppend();

                    Instant after_load_table = Instant.now();

                    // generate data
                    stmt.execute("""
                        CREATE TEMP TABLE staging_data AS
                        SELECT
                            %s
                        FROM generate_series(1, %d) AS t(x);
                    """.formatted(targetList, NUM_ROWS_PER_FILE));

                    // construct file statistics
                    Metrics metrics = computeMetrics(stmt, SCHEMA);

                    // write the data to S3 as a parquet file
                    String file_path = Path.of(table.location()).resolve(UUID.randomUUID().toString()).toString();
                    stmt.execute("""
                        COPY staging_data
                        TO '%s' (FORMAT PARQUET);
                    """.formatted(file_path));

                    stmt.execute("DROP TABLE staging_data;");

                    long file_size = getFileSize(file_path);

                    Instant after_insert_file = Instant.now();

                    DataFile newFile = DataFiles.builder(spec)
                            .withFileSizeInBytes(file_size)
                            .withFormat(FileFormat.PARQUET)
                            .withMetrics(metrics)
                            .withPath(file_path)
                            .build();

                    txn.appendFile(newFile);
                    txn.commit2(fileLogs);

                    Instant after_commit = Instant.now();

                    LOAD_TABLE_TIMES.add(new TimePair(before_load_table, after_load_table));
                    INSERT_FILE_TIMES.add(new TimePair(after_load_table, after_insert_file));
                    COMMIT_TIMES.add(new TimePair(after_insert_file, after_commit));
                    ADDED_FILES.add(fileLogs);
                }
                catch (SQLException e) {
                    System.out.println(e.getMessage());
                }
            }
        } catch (InterruptedException e) {
            // Catching interruption from the main thread
        }
    }


    public static void runTaskForDuration(Runnable task, long duration, TimeUnit unit) {
        System.out.println("starting execution for " + duration + " " + unit);

        ExecutorService executor = Executors.newSingleThreadExecutor();
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

        // Start the task
        Future<?> future = executor.submit(task);

        // Schedule the interruption
        scheduler.schedule(() -> {
            System.out.println("\nTime is up! Sending interrupt...");
            future.cancel(true); // 'true' allows interrupting the thread
            executor.shutdownNow();
            scheduler.shutdown();
        }, duration, unit);

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
        var headRequest = HeadObjectRequest.builder().bucket(bucket).key(key).build();
        return S3.headObject(headRequest).contentLength();
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
//        try (var rs = stmt.executeQuery("SELECT MIN(val), MAX(val), AVG(val) FROM staging_data")) {
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
