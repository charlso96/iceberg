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
import java.nio.file.Paths;
import java.sql.DriverManager;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalTime;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;
import org.apache.iceberg.raven.CatalogOuterClass.FileObject;
import org.apache.iceberg.raven.CatalogOuterClass.TableObject;
import org.apache.iceberg.raven.RavenCatalog;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.duckdb.DuckDBConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GCLocalRun {
    // 1. Add this private constructor
    private GCLocalRun() {}

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
            summaryNode.put("exp_name", "gclocalrun");
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

    private static final Logger LOG = LoggerFactory.getLogger(GCLocalRun.class);
    private static final ObjectMapper JSON_MAPPER = new ObjectMapper();
    private static final int NUM_TABLES = 100;

    private static String durationStr;
    private static String workspaceName;
    private static String dbName;
    private static String tableName;
    private static int numPopulators;
    private static int numThreads;
    // unlike MORWrite, compaction merges small files to a large file & expire snapshots
    // number of snapshots to expire at a time
    private static int numExpireSnapshots;
    // total number of files inserted
    private static long sleepTime;
    private static String ravenAddress;

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
        // number of snapshots to expire at a time
        numExpireSnapshots = Integer.parseInt(expConfigs.get("num_expire_snapshots"));
        // total number of files inserted
        sleepTime = Long.parseLong(expConfigs.get("sleep_time"));
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

    private static void runRavenExp(Map<String, String> expConfigs) {
        LocalTime duration = LocalTime.parse(durationStr);
        runRavenExpImpl(duration);

        String expResultDir = expConfigs.get("exp_result_dir");

        String logFileName = String.format(Locale.getDefault(),"%s/gclocal-iceberg-%d-%d-%d-log.json",
                expResultDir, numExpireSnapshots, numThreads, sleepTime);
        String summaryFileName = String.format("%s/summary.json", expResultDir);
        MetricsExporter.exportMetricsToLog(logFileName);
        MetricsExporter.appendSummaryToJson(summaryFileName);
    }

    private static void runRavenExpImpl(LocalTime duration) {
        runTaskForDuration((running, threadNum) -> {
            RavenCatalog ravenCatalog = RAVEN_CATALOGS.get(threadNum % 4);
            int tablesPerThread = NUM_TABLES / numThreads;
            int minTableNum = threadNum * tablesPerThread + 1;
            int maxTableNum = (threadNum + 1) * tablesPerThread + 1;

            Map<Integer, Integer> remainingSnapshots = Maps.newHashMap();
            for (int i = minTableNum; i < maxTableNum; i++) {
                TableObject tableObject = ravenCatalog.loadTable(workspaceName, dbName,
                        tableName + String.valueOf(i));
                remainingSnapshots.put(i, tableObject.getSnapshotVid());
            }
            int noOps = 0;
            // Keep running until flag change
            while (running.get() && noOps < (NUM_TABLES * 2)) {
                int tableNum = ThreadLocalRandom.current().nextInt(minTableNum, maxTableNum);
                // expire snapshots
                if (!expireOp(ravenCatalog, threadNum, tableNum, remainingSnapshots)) {
                    noOps++;
                }
            }
        }, duration.toSecondOfDay(), TimeUnit.SECONDS);
    }

    private static boolean expireOp(RavenCatalog ravenCatalog, Integer threadNum,
                                    int tableNum, Map<Integer, Integer> remainingSnapshots) {
        int numSnapshotsToRetain = remainingSnapshots.get(tableNum) - numExpireSnapshots;

        if (numSnapshotsToRetain > 0) {
            Instant beforeOp = Instant.now();

            String targetTableName = tableName + String.valueOf(tableNum);

            // expire snapshots
            boolean success = ravenCatalog.expireSnapshotsRetainLast(workspaceName, dbName,
                    targetTableName, numSnapshotsToRetain);

            Instant afterOp = Instant.now();

            remainingSnapshots.put(tableNum, numSnapshotsToRetain);

            // record metrics
            OP_TIMES.get(threadNum).add(new TimePair(beforeOp, afterOp));
            OP_SUCCESS.get(threadNum).add(success);
            return true;
        }

        return false;

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
}
