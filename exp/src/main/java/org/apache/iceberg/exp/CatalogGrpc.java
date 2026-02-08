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

import static io.grpc.MethodDescriptor.generateFullMethodName;

/**
 */
@javax.annotation.Generated(
        value = "by gRPC proto compiler (version 1.62.2)",
        comments = "Source: catalog.proto")
@io.grpc.stub.annotations.GrpcGenerated
public final class CatalogGrpc {

    private CatalogGrpc() {}

    public static final java.lang.String SERVICE_NAME = "Catalog";

    // Static method descriptors that strictly reflect the proto.
    private static volatile io.grpc.MethodDescriptor<CatalogOuterClass.GetTableRequest,
            CatalogOuterClass.GetTableResponse> getGetTableMethod;

    @io.grpc.stub.annotations.RpcMethod(
            fullMethodName = SERVICE_NAME + '/' + "GetTable",
            requestType = CatalogOuterClass.GetTableRequest.class,
            responseType = CatalogOuterClass.GetTableResponse.class,
            methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
    public static io.grpc.MethodDescriptor<CatalogOuterClass.GetTableRequest,
            CatalogOuterClass.GetTableResponse> getGetTableMethod() {
        io.grpc.MethodDescriptor<CatalogOuterClass.GetTableRequest, CatalogOuterClass.GetTableResponse> getGetTableMethod;
        if ((getGetTableMethod = CatalogGrpc.getGetTableMethod) == null) {
            synchronized (CatalogGrpc.class) {
                if ((getGetTableMethod = CatalogGrpc.getGetTableMethod) == null) {
                    CatalogGrpc.getGetTableMethod = getGetTableMethod =
                            io.grpc.MethodDescriptor.<CatalogOuterClass.GetTableRequest, CatalogOuterClass.GetTableResponse>newBuilder()
                                    .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
                                    .setFullMethodName(generateFullMethodName(SERVICE_NAME, "GetTable"))
                                    .setSampledToLocalTracing(true)
                                    .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                                            CatalogOuterClass.GetTableRequest.getDefaultInstance()))
                                    .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                                            CatalogOuterClass.GetTableResponse.getDefaultInstance()))
                                    .setSchemaDescriptor(new CatalogMethodDescriptorSupplier("GetTable"))
                                    .build();
                }
            }
        }
        return getGetTableMethod;
    }

    private static volatile io.grpc.MethodDescriptor<CatalogOuterClass.ListFilesRequest,
            CatalogOuterClass.ListFilesResponse> getListFilesMethod;

    @io.grpc.stub.annotations.RpcMethod(
            fullMethodName = SERVICE_NAME + '/' + "ListFiles",
            requestType = CatalogOuterClass.ListFilesRequest.class,
            responseType = CatalogOuterClass.ListFilesResponse.class,
            methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
    public static io.grpc.MethodDescriptor<CatalogOuterClass.ListFilesRequest,
            CatalogOuterClass.ListFilesResponse> getListFilesMethod() {
        io.grpc.MethodDescriptor<CatalogOuterClass.ListFilesRequest, CatalogOuterClass.ListFilesResponse> getListFilesMethod;
        if ((getListFilesMethod = CatalogGrpc.getListFilesMethod) == null) {
            synchronized (CatalogGrpc.class) {
                if ((getListFilesMethod = CatalogGrpc.getListFilesMethod) == null) {
                    CatalogGrpc.getListFilesMethod = getListFilesMethod =
                            io.grpc.MethodDescriptor.<CatalogOuterClass.ListFilesRequest, CatalogOuterClass.ListFilesResponse>newBuilder()
                                    .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
                                    .setFullMethodName(generateFullMethodName(SERVICE_NAME, "ListFiles"))
                                    .setSampledToLocalTracing(true)
                                    .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                                            CatalogOuterClass.ListFilesRequest.getDefaultInstance()))
                                    .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                                            CatalogOuterClass.ListFilesResponse.getDefaultInstance()))
                                    .setSchemaDescriptor(new CatalogMethodDescriptorSupplier("ListFiles"))
                                    .build();
                }
            }
        }
        return getListFilesMethod;
    }

    private static volatile io.grpc.MethodDescriptor<CatalogOuterClass.AppendFilesRequest,
            CatalogOuterClass.AppendFilesResponse> getAppendFilesMethod;

    @io.grpc.stub.annotations.RpcMethod(
            fullMethodName = SERVICE_NAME + '/' + "AppendFiles",
            requestType = CatalogOuterClass.AppendFilesRequest.class,
            responseType = CatalogOuterClass.AppendFilesResponse.class,
            methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
    public static io.grpc.MethodDescriptor<CatalogOuterClass.AppendFilesRequest,
            CatalogOuterClass.AppendFilesResponse> getAppendFilesMethod() {
        io.grpc.MethodDescriptor<CatalogOuterClass.AppendFilesRequest, CatalogOuterClass.AppendFilesResponse> getAppendFilesMethod;
        if ((getAppendFilesMethod = CatalogGrpc.getAppendFilesMethod) == null) {
            synchronized (CatalogGrpc.class) {
                if ((getAppendFilesMethod = CatalogGrpc.getAppendFilesMethod) == null) {
                    CatalogGrpc.getAppendFilesMethod = getAppendFilesMethod =
                            io.grpc.MethodDescriptor.<CatalogOuterClass.AppendFilesRequest, CatalogOuterClass.AppendFilesResponse>newBuilder()
                                    .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
                                    .setFullMethodName(generateFullMethodName(SERVICE_NAME, "AppendFiles"))
                                    .setSampledToLocalTracing(true)
                                    .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                                            CatalogOuterClass.AppendFilesRequest.getDefaultInstance()))
                                    .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                                            CatalogOuterClass.AppendFilesResponse.getDefaultInstance()))
                                    .setSchemaDescriptor(new CatalogMethodDescriptorSupplier("AppendFiles"))
                                    .build();
                }
            }
        }
        return getAppendFilesMethod;
    }

    private static volatile io.grpc.MethodDescriptor<CatalogOuterClass.RewriteFilesRequest,
            CatalogOuterClass.RewriteFilesResponse> getRewriteFilesMethod;

    @io.grpc.stub.annotations.RpcMethod(
            fullMethodName = SERVICE_NAME + '/' + "RewriteFiles",
            requestType = CatalogOuterClass.RewriteFilesRequest.class,
            responseType = CatalogOuterClass.RewriteFilesResponse.class,
            methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
    public static io.grpc.MethodDescriptor<CatalogOuterClass.RewriteFilesRequest,
            CatalogOuterClass.RewriteFilesResponse> getRewriteFilesMethod() {
        io.grpc.MethodDescriptor<CatalogOuterClass.RewriteFilesRequest, CatalogOuterClass.RewriteFilesResponse> getRewriteFilesMethod;
        if ((getRewriteFilesMethod = CatalogGrpc.getRewriteFilesMethod) == null) {
            synchronized (CatalogGrpc.class) {
                if ((getRewriteFilesMethod = CatalogGrpc.getRewriteFilesMethod) == null) {
                    CatalogGrpc.getRewriteFilesMethod = getRewriteFilesMethod =
                            io.grpc.MethodDescriptor.<CatalogOuterClass.RewriteFilesRequest, CatalogOuterClass.RewriteFilesResponse>newBuilder()
                                    .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
                                    .setFullMethodName(generateFullMethodName(SERVICE_NAME, "RewriteFiles"))
                                    .setSampledToLocalTracing(true)
                                    .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                                            CatalogOuterClass.RewriteFilesRequest.getDefaultInstance()))
                                    .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                                            CatalogOuterClass.RewriteFilesResponse.getDefaultInstance()))
                                    .setSchemaDescriptor(new CatalogMethodDescriptorSupplier("RewriteFiles"))
                                    .build();
                }
            }
        }
        return getRewriteFilesMethod;
    }

    private static volatile io.grpc.MethodDescriptor<CatalogOuterClass.ExecQueryRequest,
            CatalogOuterClass.ExecQueryResponse> getExecQueryMethod;

    @io.grpc.stub.annotations.RpcMethod(
            fullMethodName = SERVICE_NAME + '/' + "ExecQuery",
            requestType = CatalogOuterClass.ExecQueryRequest.class,
            responseType = CatalogOuterClass.ExecQueryResponse.class,
            methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
    public static io.grpc.MethodDescriptor<CatalogOuterClass.ExecQueryRequest,
            CatalogOuterClass.ExecQueryResponse> getExecQueryMethod() {
        io.grpc.MethodDescriptor<CatalogOuterClass.ExecQueryRequest, CatalogOuterClass.ExecQueryResponse> getExecQueryMethod;
        if ((getExecQueryMethod = CatalogGrpc.getExecQueryMethod) == null) {
            synchronized (CatalogGrpc.class) {
                if ((getExecQueryMethod = CatalogGrpc.getExecQueryMethod) == null) {
                    CatalogGrpc.getExecQueryMethod = getExecQueryMethod =
                            io.grpc.MethodDescriptor.<CatalogOuterClass.ExecQueryRequest, CatalogOuterClass.ExecQueryResponse>newBuilder()
                                    .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
                                    .setFullMethodName(generateFullMethodName(SERVICE_NAME, "ExecQuery"))
                                    .setSampledToLocalTracing(true)
                                    .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                                            CatalogOuterClass.ExecQueryRequest.getDefaultInstance()))
                                    .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                                            CatalogOuterClass.ExecQueryResponse.getDefaultInstance()))
                                    .setSchemaDescriptor(new CatalogMethodDescriptorSupplier("ExecQuery"))
                                    .build();
                }
            }
        }
        return getExecQueryMethod;
    }

    private static volatile io.grpc.MethodDescriptor<CatalogOuterClass.StartTransactionRequest,
            CatalogOuterClass.StartTransactionResponse> getStartTransactionMethod;

    @io.grpc.stub.annotations.RpcMethod(
            fullMethodName = SERVICE_NAME + '/' + "StartTransaction",
            requestType = CatalogOuterClass.StartTransactionRequest.class,
            responseType = CatalogOuterClass.StartTransactionResponse.class,
            methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
    public static io.grpc.MethodDescriptor<CatalogOuterClass.StartTransactionRequest,
            CatalogOuterClass.StartTransactionResponse> getStartTransactionMethod() {
        io.grpc.MethodDescriptor<CatalogOuterClass.StartTransactionRequest, CatalogOuterClass.StartTransactionResponse> getStartTransactionMethod;
        if ((getStartTransactionMethod = CatalogGrpc.getStartTransactionMethod) == null) {
            synchronized (CatalogGrpc.class) {
                if ((getStartTransactionMethod = CatalogGrpc.getStartTransactionMethod) == null) {
                    CatalogGrpc.getStartTransactionMethod = getStartTransactionMethod =
                            io.grpc.MethodDescriptor.<CatalogOuterClass.StartTransactionRequest, CatalogOuterClass.StartTransactionResponse>newBuilder()
                                    .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
                                    .setFullMethodName(generateFullMethodName(SERVICE_NAME, "StartTransaction"))
                                    .setSampledToLocalTracing(true)
                                    .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                                            CatalogOuterClass.StartTransactionRequest.getDefaultInstance()))
                                    .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                                            CatalogOuterClass.StartTransactionResponse.getDefaultInstance()))
                                    .setSchemaDescriptor(new CatalogMethodDescriptorSupplier("StartTransaction"))
                                    .build();
                }
            }
        }
        return getStartTransactionMethod;
    }

    private static volatile io.grpc.MethodDescriptor<CatalogOuterClass.CommitRequest,
            CatalogOuterClass.CommitResponse> getCommitMethod;

    @io.grpc.stub.annotations.RpcMethod(
            fullMethodName = SERVICE_NAME + '/' + "Commit",
            requestType = CatalogOuterClass.CommitRequest.class,
            responseType = CatalogOuterClass.CommitResponse.class,
            methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
    public static io.grpc.MethodDescriptor<CatalogOuterClass.CommitRequest,
            CatalogOuterClass.CommitResponse> getCommitMethod() {
        io.grpc.MethodDescriptor<CatalogOuterClass.CommitRequest, CatalogOuterClass.CommitResponse> getCommitMethod;
        if ((getCommitMethod = CatalogGrpc.getCommitMethod) == null) {
            synchronized (CatalogGrpc.class) {
                if ((getCommitMethod = CatalogGrpc.getCommitMethod) == null) {
                    CatalogGrpc.getCommitMethod = getCommitMethod =
                            io.grpc.MethodDescriptor.<CatalogOuterClass.CommitRequest, CatalogOuterClass.CommitResponse>newBuilder()
                                    .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
                                    .setFullMethodName(generateFullMethodName(SERVICE_NAME, "Commit"))
                                    .setSampledToLocalTracing(true)
                                    .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                                            CatalogOuterClass.CommitRequest.getDefaultInstance()))
                                    .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                                            CatalogOuterClass.CommitResponse.getDefaultInstance()))
                                    .setSchemaDescriptor(new CatalogMethodDescriptorSupplier("Commit"))
                                    .build();
                }
            }
        }
        return getCommitMethod;
    }

    private static volatile io.grpc.MethodDescriptor<CatalogOuterClass.ExpireSnapshotsRequest,
            CatalogOuterClass.ExpireSnapshotsResponse> getExpireSnapshotsMethod;

    @io.grpc.stub.annotations.RpcMethod(
            fullMethodName = SERVICE_NAME + '/' + "ExpireSnapshots",
            requestType = CatalogOuterClass.ExpireSnapshotsRequest.class,
            responseType = CatalogOuterClass.ExpireSnapshotsResponse.class,
            methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
    public static io.grpc.MethodDescriptor<CatalogOuterClass.ExpireSnapshotsRequest,
            CatalogOuterClass.ExpireSnapshotsResponse> getExpireSnapshotsMethod() {
        io.grpc.MethodDescriptor<CatalogOuterClass.ExpireSnapshotsRequest, CatalogOuterClass.ExpireSnapshotsResponse> getExpireSnapshotsMethod;
        if ((getExpireSnapshotsMethod = CatalogGrpc.getExpireSnapshotsMethod) == null) {
            synchronized (CatalogGrpc.class) {
                if ((getExpireSnapshotsMethod = CatalogGrpc.getExpireSnapshotsMethod) == null) {
                    CatalogGrpc.getExpireSnapshotsMethod = getExpireSnapshotsMethod =
                            io.grpc.MethodDescriptor.<CatalogOuterClass.ExpireSnapshotsRequest, CatalogOuterClass.ExpireSnapshotsResponse>newBuilder()
                                    .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
                                    .setFullMethodName(generateFullMethodName(SERVICE_NAME, "ExpireSnapshots"))
                                    .setSampledToLocalTracing(true)
                                    .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                                            CatalogOuterClass.ExpireSnapshotsRequest.getDefaultInstance()))
                                    .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                                            CatalogOuterClass.ExpireSnapshotsResponse.getDefaultInstance()))
                                    .setSchemaDescriptor(new CatalogMethodDescriptorSupplier("ExpireSnapshots"))
                                    .build();
                }
            }
        }
        return getExpireSnapshotsMethod;
    }

    /**
     * Creates a new async stub that supports all call types for the service
     */
    public static CatalogStub newStub(io.grpc.Channel channel) {
        io.grpc.stub.AbstractStub.StubFactory<CatalogStub> factory =
                new io.grpc.stub.AbstractStub.StubFactory<CatalogStub>() {
                    @java.lang.Override
                    public CatalogStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
                        return new CatalogStub(channel, callOptions);
                    }
                };
        return CatalogStub.newStub(factory, channel);
    }

    /**
     * Creates a new blocking-style stub that supports unary and streaming output calls on the service
     */
    public static CatalogBlockingStub newBlockingStub(
            io.grpc.Channel channel) {
        io.grpc.stub.AbstractStub.StubFactory<CatalogBlockingStub> factory =
                new io.grpc.stub.AbstractStub.StubFactory<CatalogBlockingStub>() {
                    @java.lang.Override
                    public CatalogBlockingStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
                        return new CatalogBlockingStub(channel, callOptions);
                    }
                };
        return CatalogBlockingStub.newStub(factory, channel);
    }

    /**
     * Creates a new ListenableFuture-style stub that supports unary calls on the service
     */
    public static CatalogFutureStub newFutureStub(
            io.grpc.Channel channel) {
        io.grpc.stub.AbstractStub.StubFactory<CatalogFutureStub> factory =
                new io.grpc.stub.AbstractStub.StubFactory<CatalogFutureStub>() {
                    @java.lang.Override
                    public CatalogFutureStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
                        return new CatalogFutureStub(channel, callOptions);
                    }
                };
        return CatalogFutureStub.newStub(factory, channel);
    }

    /**
     */
    public interface AsyncService {

        /**
         */
        default void getTable(CatalogOuterClass.GetTableRequest request,
                              io.grpc.stub.StreamObserver<CatalogOuterClass.GetTableResponse> responseObserver) {
            io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getGetTableMethod(), responseObserver);
        }

        /**
         */
        default void listFiles(CatalogOuterClass.ListFilesRequest request,
                               io.grpc.stub.StreamObserver<CatalogOuterClass.ListFilesResponse> responseObserver) {
            io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getListFilesMethod(), responseObserver);
        }

        /**
         */
        default void appendFiles(CatalogOuterClass.AppendFilesRequest request,
                                 io.grpc.stub.StreamObserver<CatalogOuterClass.AppendFilesResponse> responseObserver) {
            io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getAppendFilesMethod(), responseObserver);
        }

        /**
         */
        default void rewriteFiles(CatalogOuterClass.RewriteFilesRequest request,
                                  io.grpc.stub.StreamObserver<CatalogOuterClass.RewriteFilesResponse> responseObserver) {
            io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getRewriteFilesMethod(), responseObserver);
        }

        /**
         */
        default void execQuery(CatalogOuterClass.ExecQueryRequest request,
                               io.grpc.stub.StreamObserver<CatalogOuterClass.ExecQueryResponse> responseObserver) {
            io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getExecQueryMethod(), responseObserver);
        }

        /**
         */
        default void startTransaction(CatalogOuterClass.StartTransactionRequest request,
                                      io.grpc.stub.StreamObserver<CatalogOuterClass.StartTransactionResponse> responseObserver) {
            io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getStartTransactionMethod(), responseObserver);
        }

        /**
         */
        default void commit(CatalogOuterClass.CommitRequest request,
                            io.grpc.stub.StreamObserver<CatalogOuterClass.CommitResponse> responseObserver) {
            io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getCommitMethod(), responseObserver);
        }

        /**
         */
        default void expireSnapshots(CatalogOuterClass.ExpireSnapshotsRequest request,
                                     io.grpc.stub.StreamObserver<CatalogOuterClass.ExpireSnapshotsResponse> responseObserver) {
            io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getExpireSnapshotsMethod(), responseObserver);
        }
    }

    /**
     * Base class for the server implementation of the service Catalog.
     */
    public static abstract class CatalogImplBase
            implements io.grpc.BindableService, AsyncService {

        @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
            return CatalogGrpc.bindService(this);
        }
    }

    /**
     * A stub to allow clients to do asynchronous rpc calls to service Catalog.
     */
    public static final class CatalogStub
            extends io.grpc.stub.AbstractAsyncStub<CatalogStub> {
        private CatalogStub(
                io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
            super(channel, callOptions);
        }

        @java.lang.Override
        protected CatalogStub build(
                io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
            return new CatalogStub(channel, callOptions);
        }

        /**
         */
        public void getTable(CatalogOuterClass.GetTableRequest request,
                             io.grpc.stub.StreamObserver<CatalogOuterClass.GetTableResponse> responseObserver) {
            io.grpc.stub.ClientCalls.asyncUnaryCall(
                    getChannel().newCall(getGetTableMethod(), getCallOptions()), request, responseObserver);
        }

        /**
         */
        public void listFiles(CatalogOuterClass.ListFilesRequest request,
                              io.grpc.stub.StreamObserver<CatalogOuterClass.ListFilesResponse> responseObserver) {
            io.grpc.stub.ClientCalls.asyncUnaryCall(
                    getChannel().newCall(getListFilesMethod(), getCallOptions()), request, responseObserver);
        }

        /**
         */
        public void appendFiles(CatalogOuterClass.AppendFilesRequest request,
                                io.grpc.stub.StreamObserver<CatalogOuterClass.AppendFilesResponse> responseObserver) {
            io.grpc.stub.ClientCalls.asyncUnaryCall(
                    getChannel().newCall(getAppendFilesMethod(), getCallOptions()), request, responseObserver);
        }

        /**
         */
        public void rewriteFiles(CatalogOuterClass.RewriteFilesRequest request,
                                 io.grpc.stub.StreamObserver<CatalogOuterClass.RewriteFilesResponse> responseObserver) {
            io.grpc.stub.ClientCalls.asyncUnaryCall(
                    getChannel().newCall(getRewriteFilesMethod(), getCallOptions()), request, responseObserver);
        }

        /**
         */
        public void execQuery(CatalogOuterClass.ExecQueryRequest request,
                              io.grpc.stub.StreamObserver<CatalogOuterClass.ExecQueryResponse> responseObserver) {
            io.grpc.stub.ClientCalls.asyncUnaryCall(
                    getChannel().newCall(getExecQueryMethod(), getCallOptions()), request, responseObserver);
        }

        /**
         */
        public void startTransaction(CatalogOuterClass.StartTransactionRequest request,
                                     io.grpc.stub.StreamObserver<CatalogOuterClass.StartTransactionResponse> responseObserver) {
            io.grpc.stub.ClientCalls.asyncUnaryCall(
                    getChannel().newCall(getStartTransactionMethod(), getCallOptions()), request, responseObserver);
        }

        /**
         */
        public void commit(CatalogOuterClass.CommitRequest request,
                           io.grpc.stub.StreamObserver<CatalogOuterClass.CommitResponse> responseObserver) {
            io.grpc.stub.ClientCalls.asyncUnaryCall(
                    getChannel().newCall(getCommitMethod(), getCallOptions()), request, responseObserver);
        }

        /**
         */
        public void expireSnapshots(CatalogOuterClass.ExpireSnapshotsRequest request,
                                    io.grpc.stub.StreamObserver<CatalogOuterClass.ExpireSnapshotsResponse> responseObserver) {
            io.grpc.stub.ClientCalls.asyncUnaryCall(
                    getChannel().newCall(getExpireSnapshotsMethod(), getCallOptions()), request, responseObserver);
        }
    }

    /**
     * A stub to allow clients to do synchronous rpc calls to service Catalog.
     */
    public static final class CatalogBlockingStub
            extends io.grpc.stub.AbstractBlockingStub<CatalogBlockingStub> {
        private CatalogBlockingStub(
                io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
            super(channel, callOptions);
        }

        @java.lang.Override
        protected CatalogBlockingStub build(
                io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
            return new CatalogBlockingStub(channel, callOptions);
        }

        /**
         */
        public CatalogOuterClass.GetTableResponse getTable(CatalogOuterClass.GetTableRequest request) {
            return io.grpc.stub.ClientCalls.blockingUnaryCall(
                    getChannel(), getGetTableMethod(), getCallOptions(), request);
        }

        /**
         */
        public CatalogOuterClass.ListFilesResponse listFiles(CatalogOuterClass.ListFilesRequest request) {
            return io.grpc.stub.ClientCalls.blockingUnaryCall(
                    getChannel(), getListFilesMethod(), getCallOptions(), request);
        }

        /**
         */
        public CatalogOuterClass.AppendFilesResponse appendFiles(CatalogOuterClass.AppendFilesRequest request) {
            return io.grpc.stub.ClientCalls.blockingUnaryCall(
                    getChannel(), getAppendFilesMethod(), getCallOptions(), request);
        }

        /**
         */
        public CatalogOuterClass.RewriteFilesResponse rewriteFiles(CatalogOuterClass.RewriteFilesRequest request) {
            return io.grpc.stub.ClientCalls.blockingUnaryCall(
                    getChannel(), getRewriteFilesMethod(), getCallOptions(), request);
        }

        /**
         */
        public CatalogOuterClass.ExecQueryResponse execQuery(CatalogOuterClass.ExecQueryRequest request) {
            return io.grpc.stub.ClientCalls.blockingUnaryCall(
                    getChannel(), getExecQueryMethod(), getCallOptions(), request);
        }

        /**
         */
        public CatalogOuterClass.StartTransactionResponse startTransaction(CatalogOuterClass.StartTransactionRequest request) {
            return io.grpc.stub.ClientCalls.blockingUnaryCall(
                    getChannel(), getStartTransactionMethod(), getCallOptions(), request);
        }

        /**
         */
        public CatalogOuterClass.CommitResponse commit(CatalogOuterClass.CommitRequest request) {
            return io.grpc.stub.ClientCalls.blockingUnaryCall(
                    getChannel(), getCommitMethod(), getCallOptions(), request);
        }

        /**
         */
        public CatalogOuterClass.ExpireSnapshotsResponse expireSnapshots(CatalogOuterClass.ExpireSnapshotsRequest request) {
            return io.grpc.stub.ClientCalls.blockingUnaryCall(
                    getChannel(), getExpireSnapshotsMethod(), getCallOptions(), request);
        }
    }

    /**
     * A stub to allow clients to do ListenableFuture-style rpc calls to service Catalog.
     */
    public static final class CatalogFutureStub
            extends io.grpc.stub.AbstractFutureStub<CatalogFutureStub> {
        private CatalogFutureStub(
                io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
            super(channel, callOptions);
        }

        @java.lang.Override
        protected CatalogFutureStub build(
                io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
            return new CatalogFutureStub(channel, callOptions);
        }

        /**
         */
        public com.google.common.util.concurrent.ListenableFuture<CatalogOuterClass.GetTableResponse> getTable(
                CatalogOuterClass.GetTableRequest request) {
            return io.grpc.stub.ClientCalls.futureUnaryCall(
                    getChannel().newCall(getGetTableMethod(), getCallOptions()), request);
        }

        /**
         */
        public com.google.common.util.concurrent.ListenableFuture<CatalogOuterClass.ListFilesResponse> listFiles(
                CatalogOuterClass.ListFilesRequest request) {
            return io.grpc.stub.ClientCalls.futureUnaryCall(
                    getChannel().newCall(getListFilesMethod(), getCallOptions()), request);
        }

        /**
         */
        public com.google.common.util.concurrent.ListenableFuture<CatalogOuterClass.AppendFilesResponse> appendFiles(
                CatalogOuterClass.AppendFilesRequest request) {
            return io.grpc.stub.ClientCalls.futureUnaryCall(
                    getChannel().newCall(getAppendFilesMethod(), getCallOptions()), request);
        }

        /**
         */
        public com.google.common.util.concurrent.ListenableFuture<CatalogOuterClass.RewriteFilesResponse> rewriteFiles(
                CatalogOuterClass.RewriteFilesRequest request) {
            return io.grpc.stub.ClientCalls.futureUnaryCall(
                    getChannel().newCall(getRewriteFilesMethod(), getCallOptions()), request);
        }

        /**
         */
        public com.google.common.util.concurrent.ListenableFuture<CatalogOuterClass.ExecQueryResponse> execQuery(
                CatalogOuterClass.ExecQueryRequest request) {
            return io.grpc.stub.ClientCalls.futureUnaryCall(
                    getChannel().newCall(getExecQueryMethod(), getCallOptions()), request);
        }

        /**
         */
        public com.google.common.util.concurrent.ListenableFuture<CatalogOuterClass.StartTransactionResponse> startTransaction(
                CatalogOuterClass.StartTransactionRequest request) {
            return io.grpc.stub.ClientCalls.futureUnaryCall(
                    getChannel().newCall(getStartTransactionMethod(), getCallOptions()), request);
        }

        /**
         */
        public com.google.common.util.concurrent.ListenableFuture<CatalogOuterClass.CommitResponse> commit(
                CatalogOuterClass.CommitRequest request) {
            return io.grpc.stub.ClientCalls.futureUnaryCall(
                    getChannel().newCall(getCommitMethod(), getCallOptions()), request);
        }

        /**
         */
        public com.google.common.util.concurrent.ListenableFuture<CatalogOuterClass.ExpireSnapshotsResponse> expireSnapshots(
                CatalogOuterClass.ExpireSnapshotsRequest request) {
            return io.grpc.stub.ClientCalls.futureUnaryCall(
                    getChannel().newCall(getExpireSnapshotsMethod(), getCallOptions()), request);
        }
    }

    private static final int METHODID_GET_TABLE = 0;
    private static final int METHODID_LIST_FILES = 1;
    private static final int METHODID_APPEND_FILES = 2;
    private static final int METHODID_REWRITE_FILES = 3;
    private static final int METHODID_EXEC_QUERY = 4;
    private static final int METHODID_START_TRANSACTION = 5;
    private static final int METHODID_COMMIT = 6;
    private static final int METHODID_EXPIRE_SNAPSHOTS = 7;

    private static final class MethodHandlers<Req, Resp> implements
            io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>,
            io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>,
            io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>,
            io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {
        private final AsyncService serviceImpl;
        private final int methodId;

        MethodHandlers(AsyncService serviceImpl, int methodId) {
            this.serviceImpl = serviceImpl;
            this.methodId = methodId;
        }

        @java.lang.Override
        @java.lang.SuppressWarnings("unchecked")
        public void invoke(Req request, io.grpc.stub.StreamObserver<Resp> responseObserver) {
            switch (methodId) {
                case METHODID_GET_TABLE:
                    serviceImpl.getTable((CatalogOuterClass.GetTableRequest) request,
                            (io.grpc.stub.StreamObserver<CatalogOuterClass.GetTableResponse>) responseObserver);
                    break;
                case METHODID_LIST_FILES:
                    serviceImpl.listFiles((CatalogOuterClass.ListFilesRequest) request,
                            (io.grpc.stub.StreamObserver<CatalogOuterClass.ListFilesResponse>) responseObserver);
                    break;
                case METHODID_APPEND_FILES:
                    serviceImpl.appendFiles((CatalogOuterClass.AppendFilesRequest) request,
                            (io.grpc.stub.StreamObserver<CatalogOuterClass.AppendFilesResponse>) responseObserver);
                    break;
                case METHODID_REWRITE_FILES:
                    serviceImpl.rewriteFiles((CatalogOuterClass.RewriteFilesRequest) request,
                            (io.grpc.stub.StreamObserver<CatalogOuterClass.RewriteFilesResponse>) responseObserver);
                    break;
                case METHODID_EXEC_QUERY:
                    serviceImpl.execQuery((CatalogOuterClass.ExecQueryRequest) request,
                            (io.grpc.stub.StreamObserver<CatalogOuterClass.ExecQueryResponse>) responseObserver);
                    break;
                case METHODID_START_TRANSACTION:
                    serviceImpl.startTransaction((CatalogOuterClass.StartTransactionRequest) request,
                            (io.grpc.stub.StreamObserver<CatalogOuterClass.StartTransactionResponse>) responseObserver);
                    break;
                case METHODID_COMMIT:
                    serviceImpl.commit((CatalogOuterClass.CommitRequest) request,
                            (io.grpc.stub.StreamObserver<CatalogOuterClass.CommitResponse>) responseObserver);
                    break;
                case METHODID_EXPIRE_SNAPSHOTS:
                    serviceImpl.expireSnapshots((CatalogOuterClass.ExpireSnapshotsRequest) request,
                            (io.grpc.stub.StreamObserver<CatalogOuterClass.ExpireSnapshotsResponse>) responseObserver);
                    break;
                default:
                    throw new AssertionError();
            }
        }

        @java.lang.Override
        @java.lang.SuppressWarnings("unchecked")
        public io.grpc.stub.StreamObserver<Req> invoke(
                io.grpc.stub.StreamObserver<Resp> responseObserver) {
            switch (methodId) {
                default:
                    throw new AssertionError();
            }
        }
    }

    public static final io.grpc.ServerServiceDefinition bindService(AsyncService service) {
        return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
                .addMethod(
                        getGetTableMethod(),
                        io.grpc.stub.ServerCalls.asyncUnaryCall(
                                new MethodHandlers<
                                        CatalogOuterClass.GetTableRequest,
                                        CatalogOuterClass.GetTableResponse>(
                                        service, METHODID_GET_TABLE)))
                .addMethod(
                        getListFilesMethod(),
                        io.grpc.stub.ServerCalls.asyncUnaryCall(
                                new MethodHandlers<
                                        CatalogOuterClass.ListFilesRequest,
                                        CatalogOuterClass.ListFilesResponse>(
                                        service, METHODID_LIST_FILES)))
                .addMethod(
                        getAppendFilesMethod(),
                        io.grpc.stub.ServerCalls.asyncUnaryCall(
                                new MethodHandlers<
                                        CatalogOuterClass.AppendFilesRequest,
                                        CatalogOuterClass.AppendFilesResponse>(
                                        service, METHODID_APPEND_FILES)))
                .addMethod(
                        getRewriteFilesMethod(),
                        io.grpc.stub.ServerCalls.asyncUnaryCall(
                                new MethodHandlers<
                                        CatalogOuterClass.RewriteFilesRequest,
                                        CatalogOuterClass.RewriteFilesResponse>(
                                        service, METHODID_REWRITE_FILES)))
                .addMethod(
                        getExecQueryMethod(),
                        io.grpc.stub.ServerCalls.asyncUnaryCall(
                                new MethodHandlers<
                                        CatalogOuterClass.ExecQueryRequest,
                                        CatalogOuterClass.ExecQueryResponse>(
                                        service, METHODID_EXEC_QUERY)))
                .addMethod(
                        getStartTransactionMethod(),
                        io.grpc.stub.ServerCalls.asyncUnaryCall(
                                new MethodHandlers<
                                        CatalogOuterClass.StartTransactionRequest,
                                        CatalogOuterClass.StartTransactionResponse>(
                                        service, METHODID_START_TRANSACTION)))
                .addMethod(
                        getCommitMethod(),
                        io.grpc.stub.ServerCalls.asyncUnaryCall(
                                new MethodHandlers<
                                        CatalogOuterClass.CommitRequest,
                                        CatalogOuterClass.CommitResponse>(
                                        service, METHODID_COMMIT)))
                .addMethod(
                        getExpireSnapshotsMethod(),
                        io.grpc.stub.ServerCalls.asyncUnaryCall(
                                new MethodHandlers<
                                        CatalogOuterClass.ExpireSnapshotsRequest,
                                        CatalogOuterClass.ExpireSnapshotsResponse>(
                                        service, METHODID_EXPIRE_SNAPSHOTS)))
                .build();
    }

    private static abstract class CatalogBaseDescriptorSupplier
            implements io.grpc.protobuf.ProtoFileDescriptorSupplier, io.grpc.protobuf.ProtoServiceDescriptorSupplier {
        CatalogBaseDescriptorSupplier() {}

        @java.lang.Override
        public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
            return CatalogOuterClass.getDescriptor();
        }

        @java.lang.Override
        public com.google.protobuf.Descriptors.ServiceDescriptor getServiceDescriptor() {
            return getFileDescriptor().findServiceByName("Catalog");
        }
    }

    private static final class CatalogFileDescriptorSupplier
            extends CatalogBaseDescriptorSupplier {
        CatalogFileDescriptorSupplier() {}
    }

    private static final class CatalogMethodDescriptorSupplier
            extends CatalogBaseDescriptorSupplier
            implements io.grpc.protobuf.ProtoMethodDescriptorSupplier {
        private final java.lang.String methodName;

        CatalogMethodDescriptorSupplier(java.lang.String methodName) {
            this.methodName = methodName;
        }

        @java.lang.Override
        public com.google.protobuf.Descriptors.MethodDescriptor getMethodDescriptor() {
            return getServiceDescriptor().findMethodByName(methodName);
        }
    }

    private static volatile io.grpc.ServiceDescriptor serviceDescriptor;

    public static io.grpc.ServiceDescriptor getServiceDescriptor() {
        io.grpc.ServiceDescriptor result = serviceDescriptor;
        if (result == null) {
            synchronized (CatalogGrpc.class) {
                result = serviceDescriptor;
                if (result == null) {
                    serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
                            .setSchemaDescriptor(new CatalogFileDescriptorSupplier())
                            .addMethod(getGetTableMethod())
                            .addMethod(getListFilesMethod())
                            .addMethod(getAppendFilesMethod())
                            .addMethod(getRewriteFilesMethod())
                            .addMethod(getExecQueryMethod())
                            .addMethod(getStartTransactionMethod())
                            .addMethod(getCommitMethod())
                            .addMethod(getExpireSnapshotsMethod())
                            .build();
                }
            }
        }
        return result;
    }
}
