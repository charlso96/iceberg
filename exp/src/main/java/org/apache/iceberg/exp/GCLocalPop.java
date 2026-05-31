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
import java.nio.file.Files;
import java.nio.file.Path;
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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import org.apache.iceberg.Schema;
import org.apache.iceberg.raven.CatalogOuterClass.FileObject;
import org.apache.iceberg.raven.CatalogOuterClass.TableObject;
import org.apache.iceberg.raven.RavenCatalog;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.duckdb.DuckDBConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GCLocalPop {
    // 1. Add this private constructor
    private GCLocalPop() {}

    public static class MetricsExporter {
        public static void exportMetricsToLog(String outputFile) {
            ObjectMapper mapper = new ObjectMapper();
            mapper.registerModule(new JavaTimeModule());
            mapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

            // IMPORTANT: Disable pretty printing.
            // NDJSON requires each object to be on exactly one line.
            // If you pretty print, the newlines inside the object will break the format.
            try (BufferedWriter writer = Files.newBufferedWriter(Paths.get(outputFile), UTF_8)) {
                for (int i = 0; i < OP_TIMES.size(); i++) {
                    for (int j = 0; j < OP_TIMES.get(i).size(); j++) {
                        // 1. Build the object
                        ObjectNode row = mapper.createObjectNode();
                        row.put("threadNum", i);
                        row.put("success", OP_SUCCESS.get(i).get(j));
                        addTimeBlock(mapper, row, "commit", OP_TIMES.get(i).get(j));

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
            for (List<TimePair> commitTimes : OP_TIMES) {
                numTxn += commitTimes.size();
            }

            // 3. Calculate avg_latency
            double totalLatencyMillis = 0;

            for (int i = 0; i < OP_TIMES.size(); i++) {
                for (int j = 0; j < OP_TIMES.get(i).size(); j++) {
                    // Start: Always LOAD_TABLE start
                    Instant start = OP_TIMES.get(i).get(j).start;

                    // End: COMPACT end if available, otherwise COMMIT end
                    Instant end = OP_TIMES.get(i).get(j).end;

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
            summaryNode.put("exp_name", "gclocalpop");
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

    private static final Logger LOG = LoggerFactory.getLogger(GCLocalPop.class);
    private static final ObjectMapper JSON_MAPPER = new ObjectMapper();
    private static final int NUM_TABLES = 100;

    private static String durationStr;
    private static String workspaceName;
    private static String dbName;
    private static String tableName;
    private static int numPopulators;
    private static int numThreads;
    // unlike MORWrite, compaction merges small files to a large file & expire snapshots
    private static int txnPerCompaction;
    private static int numRowsPerFile;
    // number of snapshots to expire at a time
    private static int numExpireSnapshots;
    // total number of files inserted
    private static int numFiles;
    private static long sleepTime;
    private static String warehouseLocation;
    private static String ravenAddress;
    private static final Schema SCHEMA = TPCDSSchema.STORE_SALES;

    private static final List<RavenCatalog> RAVEN_CATALOGS = Lists.newArrayList();
    private static final List<DuckDBConnection> DUCK_DB_CONNECTIONS = Lists.newArrayList();
    // data structure for measurements
    private static final List<List<TimePair>> OP_TIMES = Lists.newArrayList();
    private static final List<List<Boolean>> OP_SUCCESS = Lists.newArrayList();

    public static Map<String, String> parseJsonToMap(String jsonFilePath) throws IOException {
        File file = new File(jsonFilePath);

        // TypeReference is essential to tell Jackson the specific
        // Map implementation and generic types to use.
        return JSON_MAPPER.readValue(file, new TypeReference<Map<String, String>>() {});
    }

    public static void main(String[] args) throws Exception {
        // System.setProperty("datanucleus.plugin.pluginRegistryBundleCheck", "LOG");
        // read the config file for experimentation
        Map<String, String> expConfigs = parseJsonToMap(args[0]);

        durationStr = expConfigs.get("duration");
        workspaceName = expConfigs.get("workspace_name");
        dbName = expConfigs.get("db_name");
        tableName = expConfigs.get("table_name");
        numPopulators = Integer.parseInt(expConfigs.get("num_populators"));
        numThreads = Integer.parseInt(expConfigs.get("num_threads"));
        txnPerCompaction = Integer.parseInt(expConfigs.get("txn_per_compaction"));
        numRowsPerFile = Integer.parseInt(expConfigs.get("num_rows_per_file"));
        // number of snapshots to expire at a time
        numExpireSnapshots = Integer.parseInt(expConfigs.get("num_expire_snapshots"));
        // total number of files inserted
        numFiles = Integer.parseInt(expConfigs.get("num_files"));
        sleepTime = Long.parseLong(expConfigs.get("sleep_time"));
        warehouseLocation = expConfigs.get("warehouse_location");
        ravenAddress = expConfigs.get("raven_address");

        for (int i = 0; i < numPopulators; i++) {
            DUCK_DB_CONNECTIONS.add((DuckDBConnection) DriverManager.getConnection("jdbc:duckdb:"));
        }

        int numRaven = Math.min(numThreads, 4);
        for (int i = 0; i < numRaven; i++) {
            RAVEN_CATALOGS.add(new RavenCatalog(ravenAddress));
        }

        for (int i = 0; i < numThreads; i++) {
            List<TimePair> commitTimes = Lists.newArrayList();
            OP_TIMES.add(commitTimes);
            List<Boolean> opSuccess = Lists.newArrayList();
            OP_SUCCESS.add(opSuccess);
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
        runRavenExpImpl();

        String expResultDir = expConfigs.get("exp_result_dir");

        String logFileName = String.format(Locale.getDefault(),"%s/gclocal-iceberg-%d-%d-%d-log.json",
                expResultDir, numExpireSnapshots, numThreads, sleepTime);
        String summaryFileName = String.format("%s/summary.json", expResultDir);
        MetricsExporter.exportMetricsToLog(logFileName);
        MetricsExporter.appendSummaryToJson(summaryFileName);
    }

    private static void runRavenExpImpl() {
        ExecutorService populator = Executors.newFixedThreadPool(numPopulators);
        // populate with files
        List<Callable<Boolean>> populateTasks = Lists.newArrayList();
        for (int i = 0; i < numPopulators; i++) {
            int finalI = i;
            populateTasks.add(() -> populateImpl(finalI));
        }

        try {
            populator.invokeAll(populateTasks);
        }
        catch (InterruptedException e) {
            LOG.info("InterruptedException", e);
        }

        LOG.info("Populated all the files");

        populator.shutdown();
    }

    private static Boolean populateImpl(int threadNum) {
        DuckDBConnection duckDbConn = DUCK_DB_CONNECTIONS.get(threadNum);
        RavenCatalog ravenCatalog = RAVEN_CATALOGS.get(threadNum % 4);
        int tablesPerThread = NUM_TABLES / numPopulators;
        int minTableNum = threadNum * tablesPerThread + 1;
        int maxTableNum = (threadNum + 1) * tablesPerThread + 1;

        for (int i = 0; i < numFiles / numPopulators; i++) {
            boolean success = false;
            int retry = 0;
            while (!success && retry < 5) {
                try (Statement stmt = duckDbConn.createStatement()) {
                    int tableNum = ThreadLocalRandom.current().nextInt(minTableNum, maxTableNum);
                    String targetTableName = tableName + String.valueOf(tableNum);

                    TableObject tableObject = ravenCatalog.loadTable(workspaceName, dbName, targetTableName);

                    String targetList = schemaToTargetList(SCHEMA);
                    // first time insert a file since there is nothing to compact

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

                    }
                    // compact files
                    else {
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

                    }
                } catch (SQLException e) {
                    LOG.info("Database error occurred", e);
                }
                retry += 1;
            }
        }

        return true;
    }

    public static void deleteFiles(List<String> filePaths) {
        if (filePaths == null || filePaths.isEmpty()) {
            return;
        }

        for (String filePath : filePaths) {
            try {
                Path path = Paths.get(filePath);
                // deleteIfExists is preferred as it gracefully handles cases where the file is already gone
                Files.deleteIfExists(path);
            } catch (IOException e) {
                System.err.println("Failed to delete " + filePath + ": " + e.getMessage());
            }
        }
    }

}
