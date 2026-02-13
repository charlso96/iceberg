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

import java.util.List;
import java.util.Map;
import org.apache.iceberg.raven.RavenCatalog;
import org.apache.iceberg.raven.CatalogOuterClass.*;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestRavenCatalog {
    private static final Logger LOG = LoggerFactory.getLogger(TestRavenCatalog.class);
    private static RavenCatalog ravenCatalog;

    private TestRavenCatalog() {}

    public static void main(String[] args) throws Exception {
        Map<String, String> configs = Maps.newHashMap();
        configs.put("raven.address", "localhost:9876");
        ravenCatalog = new RavenCatalog(configs);
        test2();
        test3();
    }

//    private static void test1() {
//        TableObject table = ravenCatalog.loadTable("workspace1", "db1", "table1");
//        LOG.info("Table name: {}", table.getTableName());
//        List<FileObject> filesToAdd = Lists.newArrayList();
//        FileObject newFile = FileObject.newBuilder().setPath("s3://testraven/foo3.parquet").setSize(100).build();
//        filesToAdd.add(newFile);
//        boolean appended = ravenCatalog.appendFiles(table, filesToAdd);
//        if (appended) {
//            LOG.info("Appended {} to {}", newFile.getPath(), table.getTableName());
//        }
//        table = ravenCatalog.loadTable("workspace1", "db1", "table1");
//        List<FileObject> filesList = ravenCatalog.listFiles(table);
//        for (FileObject file : filesList) {
//            LOG.info("{}", file.getPath());
//        }
//    }

    private static void test2() {
        List<String> tableNames = Lists.newArrayList();
        tableNames.add("table1");
        tableNames.add("table2");
        StartTransactionResponse txn = ravenCatalog.startTransaction("workspace1", "db1", tableNames);
        long txnId = txn.getTxnId();
        List<TableObject> tables = txn.getTablesList();
        TableObject table1 = tables.get(0);
        TableObject table2 = tables.get(1);

        // First append to table1
        List<FileObject> filesToAdd = Lists.newArrayList();
        FileObject newFile = FileObject.newBuilder().setPath("s3://testraven/foo1.parquet").setSize(100).build();
        filesToAdd.add(newFile);
        boolean appended = ravenCatalog.appendFiles(table1, filesToAdd, txnId);
        if (appended) {
            LOG.info("Appended {} to {}", newFile.getPath(), table1.getTableName());
        }

        // Second final append to table1
        filesToAdd = Lists.newArrayList();
        newFile = FileObject.newBuilder().setPath("s3://testraven/foo2.parquet").setSize(100).build();
        filesToAdd.add(newFile);
        appended = ravenCatalog.finalAppendFiles(table1, filesToAdd, txnId);
        if (appended) {
            LOG.info("Appended {} to {}", newFile.getPath(), table1.getTableName());
        }

        // Single append to table2
        filesToAdd = Lists.newArrayList();
        newFile = FileObject.newBuilder().setPath("s3://testraven/foo3.parquet").setSize(100).build();
        filesToAdd.add(newFile);
        appended = ravenCatalog.finalAppendFiles(table2, filesToAdd, txnId);
        if (appended) {
            LOG.info("Appended {} to {}", newFile.getPath(), table2.getTableName());
        }

        // commit
        appended = ravenCatalog.commit(txnId);
        if (appended) {
            LOG.info("Committed");
        }

//        table1 = ravenCatalog.loadTable("workspace1", "db1", "table1");
//        List<FileObject> filesList = ravenCatalog.listFiles(table1);
//        for (FileObject file : filesList) {
//            LOG.info("{}", file.getPath());
//        }
//        table2 = ravenCatalog.loadTable("workspace1", "db1", "table2");
//        filesList = ravenCatalog.listFiles(table2);
//        for (FileObject file : filesList) {
//            LOG.info("{}", file.getPath());
//        }
    }

    private static void test3() {
        List<String> tableNames = Lists.newArrayList();
        tableNames.add("table1");
        tableNames.add("table2");
        StartTransactionResponse txn = ravenCatalog.startTransaction("workspace1", "db1", tableNames);
        long txnId = txn.getTxnId();
        List<TableObject> tables = txn.getTablesList();
        TableObject table1 = tables.get(0);
        TableObject table2 = tables.get(1);

        // First rewriting to table1
        List<FileObject> filesToAdd = Lists.newArrayList();
        FileObject newFile = FileObject.newBuilder().setPath("s3://testraven/foo4.parquet").setSize(100).build();
        filesToAdd.add(newFile);
        List<String> filesToReplace = Lists.newArrayList();
        filesToReplace.add("s3://testraven/foo1.parquet");
        boolean rewrote = ravenCatalog.rewriteFiles(table1, filesToReplace, filesToAdd, txnId);
        if (rewrote) {
            LOG.info("Rewrote files in Table1");
        }

        // Second rewriting to table1
        filesToReplace = Lists.newArrayList();
        filesToReplace.add("s3://testraven/foo2.parquet");
        filesToAdd = Lists.newArrayList();
        rewrote = ravenCatalog.finalRewriteFiles(table1, filesToReplace, filesToAdd, txnId);
        if (rewrote) {
            LOG.info("Rewrote files in Table1");
        }

        // Single rewriting to table2
        filesToReplace = Lists.newArrayList();
        filesToReplace.add("s3://testraven/foo3.parquet");
        rewrote = ravenCatalog.finalRewriteFiles(table2, filesToReplace, filesToAdd, txnId);
        if (rewrote) {
            LOG.info("Rewrote files in Table2");
        }

        // commit
        rewrote = ravenCatalog.commit(txnId);
        if (rewrote) {
            LOG.info("Committed");
        }

        table1 = ravenCatalog.loadTable("workspace1", "db1", "table1");
        List<FileObject> filesList = ravenCatalog.listFiles(table1);
        for (FileObject file : filesList) {
            LOG.info("{}", file.getPath());
        }
        table2 = ravenCatalog.loadTable("workspace1", "db1", "table2");
        filesList = ravenCatalog.listFiles(table2);
        for (FileObject file : filesList) {
            LOG.info("{}", file.getPath());
        }
    }
}
