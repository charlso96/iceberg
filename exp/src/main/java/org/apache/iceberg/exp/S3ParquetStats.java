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

import java.net.URI;
import java.util.Map;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.SeekableInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

public class S3ParquetStats {
    private S3ParquetStats() {}

    private static final Logger LOG = LoggerFactory.getLogger(S3ParquetStats.class);

    public static Metrics extractStats(S3Client s3, String path) {
        URI uri = URI.create(path);
        String bucket = uri.getHost();
        String key = uri.getPath().substring(1);
        long numRowsPerFile = 0;
        Map<Integer, Long> valueCounts = Maps.newHashMap();
        Map<Integer, Long> nullValueCounts = Maps.newHashMap();
        Map<Integer, Long> nanValueCounts = Maps.newHashMap();

        try {
            // 1. Create the bridge to S3
            InputFile s3File = new S3InputFile(s3, bucket, key);

            // 2. Read the footer (metadata) only
            // standard ParquetFileReader works here without Hadoop Config!
            try (ParquetFileReader reader = ParquetFileReader.open(s3File)) {
                ParquetMetadata metadata = reader.getFooter();
                // 3. Iterate over Row Groups (Blocks)
                for (BlockMetaData block : metadata.getBlocks()) {
                    long blockRowCount = block.getRowCount();
                    numRowsPerFile += blockRowCount;
                    // 4. Iterate over Columns
                    List<ColumnChunkMetaData> blockColumns = block.getColumns();
                    // initial identities
                    if (valueCounts.isEmpty()) {
                        for (int i = 1; i < blockColumns.size() + 1; i++) {
                            valueCounts.put(i, 0L);
                            nullValueCounts.put(i, 0L);
                            nanValueCounts.put(i, 0L);
                        }
                    }

                    for (int i = 0; i < blockColumns.size(); i++) {
                        Statistics<?> stats = blockColumns.get(i).getStatistics();
                        long numNulls = stats.getNumNulls();
                        valueCounts.put(i + 1, valueCounts.get(i + 1) + blockRowCount - numNulls);
                        nullValueCounts.put(i + 1, nullValueCounts.get(i + 1) + numNulls);
                    }
                }
            }
        } catch (IOException e) {
            LOG.info("IO Exception", e);
        }

        return new Metrics(numRowsPerFile, null, valueCounts, nullValueCounts, nanValueCounts);
    }

//    private static void printColumnStats(ColumnChunkMetaData column) {
//        String colName = column.getPath().toDotString();
//        Statistics<?> stats = column.getStatistics();
//
//        if (stats == null || stats.isEmpty()) {
//            LOG.info("{}: [No Stats]", colName);
//            return;
//        }
//
//        Object min = getTypedValue(stats.getMinBytes(), column.getPrimitiveType());
//        Object max = getTypedValue(stats.getMaxBytes(), column.getPrimitiveType());
//
//        String columnHeadings = String.format(Locale.getDefault(),
//                "%-20s | Type: %-10s | Nulls: %d | Min: %s | Max: %s%n",
//                colName,
//                column.getPrimitiveType().getPrimitiveTypeName(),
//                stats.getNumNulls(),
//                min,
//                max);
//        LOG.info("{}", columnHeadings);
//    }

//    /**
//     * Converts raw bytes into Java types, handling DECIMALS specifically.
//     */
//    private static Object getTypedValue(byte[] bytes, PrimitiveType type) {
//        if (bytes == null || bytes.length == 0) return "null";
//
//        // HANDLE DECIMAL
//        LogicalTypeAnnotation annotation = type.getLogicalTypeAnnotation();
//        if (annotation instanceof LogicalTypeAnnotation.DecimalLogicalTypeAnnotation) {
//            LogicalTypeAnnotation.DecimalLogicalTypeAnnotation decimal =
//                    (LogicalTypeAnnotation.DecimalLogicalTypeAnnotation) annotation;
//
//            int scale = decimal.getScale();
//            // Convert Big-Endian bytes to BigInteger, then to BigDecimal with scale
//            return new BigDecimal(new BigInteger(bytes), scale);
//        }
//
//        // Handle other simple types for readable output
//        switch (type.getPrimitiveTypeName()) {
//            case INT32: return ByteBuffer.wrap(bytes).order(java.nio.ByteOrder.LITTLE_ENDIAN).getInt();
//            case INT64: return ByteBuffer.wrap(bytes).order(java.nio.ByteOrder.LITTLE_ENDIAN).getLong();
//            case FLOAT: return ByteBuffer.wrap(bytes).order(java.nio.ByteOrder.LITTLE_ENDIAN).getFloat();
//            case DOUBLE: return ByteBuffer.wrap(bytes).order(java.nio.ByteOrder.LITTLE_ENDIAN).getDouble();
//            case BINARY: return new String(bytes, StandardCharsets.UTF_8); // Assuming UTF-8 string for simplicity
//            case BOOLEAN: return bytes[0] != 0;
//            default: return "binary_data";
//        }
//    }

    // =================================================================================
    // ADAPTER CLASS 1: Connects Parquet InputFile interface to S3
    // =================================================================================
    public static class S3InputFile implements InputFile {
        private final S3Client s3;
        private final String bucket;
        private final String key;
        private final long length;

        public S3InputFile(S3Client s3, String bucket, String key) {
            this.s3 = s3;
            this.bucket = bucket;
            this.key = key;
            // Get file size once to avoid repeated HEAD requests
            this.length = s3.headObject(HeadObjectRequest.builder().bucket(bucket).key(key).build()).contentLength();
        }

        @Override
        public long getLength() {
            return length;
        }

        @Override
        public SeekableInputStream newStream() {
            return new S3SeekableInputStream(s3, bucket, key, length);
        }
    }

    // =================================================================================
    // ADAPTER CLASS 2: Handles seeking by using S3 Range headers
    // =================================================================================
    public static class S3SeekableInputStream extends SeekableInputStream {
        private final S3Client s3;
        private final String bucket;
        private final String key;
        private final long contentLength;
        private long pos = 0;

        public S3SeekableInputStream(S3Client s3, String bucket, String key, long contentLength) {
            this.s3 = s3;
            this.bucket = bucket;
            this.key = key;
            this.contentLength = contentLength;
        }

        @Override
        public long getPos() {
            return pos;
        }

        @Override
        public void seek(long newPos) {
            this.pos = newPos;
        }

        @Override
        public void readFully(byte[] bytes) throws IOException {
            readFully(bytes, 0, bytes.length);
        }

        @Override
        public void readFully(byte[] bytes, int start, int len) throws IOException {
            // Parquet often asks for data at random offsets.
            // We use the S3 "Range" header to fetch ONLY the bytes requested.
            String range = "bytes=" + pos + "-" + (pos + len - 1);

            try (ResponseInputStream<GetObjectResponse> responseStream = s3.getObject(GetObjectRequest.builder()
                    .bucket(bucket)
                    .key(key)
                    .range(range)
                    .build())) {

                int read = 0;
                while (read < len) {
                    int n = responseStream.read(bytes, start + read, len - read);
                    if (n < 0) throw new IOException("Unexpected EOF from S3");
                    read += n;
                }
                pos += len;
            }
        }

        @Override
        public int read() throws IOException {
            // Inefficient but required by interface. Reads 1 byte.
            byte[] b = new byte[1];
            readFully(b);
            return b[0] & 0xFF;
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            if (len == 0) {
                return 0;
            }

            // Cap the length to avoid reading past the end of the file
            long remainingInFile = contentLength - pos;
            if (remainingInFile <= 0) {
                return -1; // EOF
            }

            // We can only read as much as is left in the file
            int bytesToRead = (int) Math.min(len, remainingInFile);

            // Reuse the efficient "Range" fetching logic
            // (We implement this manually here to return bytes read instead of void)
            String range = "bytes=" + pos + "-" + (pos + bytesToRead - 1);

            try (ResponseInputStream<GetObjectResponse> responseStream =
                         s3.getObject(software.amazon.awssdk.services.s3.model.GetObjectRequest.builder()
                    .bucket(bucket)
                    .key(key)
                    .range(range)
                    .build())) {

                int totalRead = 0;
                while (totalRead < bytesToRead) {
                    int n = responseStream.read(b, off + totalRead, bytesToRead - totalRead);
                    if (n < 0) break; // Should not happen given range request, but safe to check
                    totalRead += n;
                }

                pos += totalRead;
                return totalRead;

            } catch (software.amazon.awssdk.services.s3.model.S3Exception e) {
                // Handle 416 Range Not Satisfiable or other issues
                throw new IOException("Failed to read from S3", e);
            }
        }

        @Override
        public void readFully(ByteBuffer byteBuffer) throws IOException {
            if (byteBuffer.hasArray()) {
                readFully(byteBuffer.array(), byteBuffer.arrayOffset() + byteBuffer.position(), byteBuffer.remaining());
                byteBuffer.position(byteBuffer.limit());
            } else {
                byte[] temp = new byte[byteBuffer.remaining()];
                readFully(temp);
                byteBuffer.put(temp);
            }
        }

        @Override
        public int read(ByteBuffer buf) throws IOException {
            int bytesToRead = buf.remaining();
            if (bytesToRead == 0) {
                return 0;
            }

            // Allocate a temporary byte array to read from S3
            byte[] temp = new byte[bytesToRead];

            // Reuse our existing readFully implementation to fetch the data
            try {
                readFully(temp);
            } catch (java.io.EOFException e) {
                return -1; // Return -1 to indicate End of Stream per InputStream contract
            }

            // Transfer data to the ByteBuffer
            buf.put(temp);

            return bytesToRead;
        }
    }
}
