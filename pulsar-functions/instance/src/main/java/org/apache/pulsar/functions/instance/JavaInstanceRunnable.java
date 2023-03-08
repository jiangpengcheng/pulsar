/**
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

package org.apache.pulsar.functions.instance;

import static org.apache.pulsar.functions.utils.FunctionCommon.convertFromFunctionDetailsSubscriptionPosition;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.base.Preconditions;
import com.google.protobuf.ByteString;
import com.google.protobuf.Empty;
import com.google.protobuf.util.JsonFormat;
import com.scurrilous.circe.checksum.Crc32cIntChecksum;
import io.grpc.Server;
import io.grpc.netty.NettyServerBuilder;
import io.grpc.stub.StreamObserver;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerDomainSocketChannel;
import io.netty.channel.kqueue.KQueueEventLoopGroup;
import io.netty.channel.kqueue.KQueueServerDomainSocketChannel;
import io.netty.channel.unix.DomainSocketAddress;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import lombok.Data;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import net.jodah.typetools.TypeResolver;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.ThreadContext;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.ClientBuilder;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.common.functions.ConsumerConfig;
import org.apache.pulsar.common.functions.FunctionConfig;
import org.apache.pulsar.common.functions.ProducerConfig;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.apache.pulsar.common.util.Reflections;
import org.apache.pulsar.functions.api.Function;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.functions.api.StateStore;
import org.apache.pulsar.functions.api.StateStoreContext;
import org.apache.pulsar.functions.instance.state.BKStateStoreProviderImpl;
import org.apache.pulsar.functions.instance.state.InstanceStateManager;
import org.apache.pulsar.functions.instance.state.StateManager;
import org.apache.pulsar.functions.instance.state.StateStoreContextImpl;
import org.apache.pulsar.functions.instance.state.StateStoreProvider;
import org.apache.pulsar.functions.instance.stats.ComponentStatsManager;
import org.apache.pulsar.functions.instance.stats.FunctionCollectorRegistry;
import org.apache.pulsar.functions.proto.ContextGrpc;
import org.apache.pulsar.functions.proto.Function.SinkSpec;
import org.apache.pulsar.functions.proto.Function.SourceSpec;
import org.apache.pulsar.functions.proto.InstanceCommunication;
import org.apache.pulsar.functions.proto.InstanceCommunication.MetricsData.Builder;
import org.apache.pulsar.functions.proto.InstanceContext;
import org.apache.pulsar.functions.secretsprovider.SecretsProvider;
import org.apache.pulsar.functions.sink.PulsarSink;
import org.apache.pulsar.functions.sink.PulsarSinkConfig;
import org.apache.pulsar.functions.sink.PulsarSinkDisable;
import org.apache.pulsar.functions.source.MultiConsumerPulsarSource;
import org.apache.pulsar.functions.source.MultiConsumerPulsarSourceConfig;
import org.apache.pulsar.functions.source.PulsarSource;
import org.apache.pulsar.functions.source.PulsarSourceConfig;
import org.apache.pulsar.functions.source.SingleConsumerPulsarSource;
import org.apache.pulsar.functions.source.SingleConsumerPulsarSourceConfig;
import org.apache.pulsar.functions.source.batch.BatchSourceExecutor;
import org.apache.pulsar.functions.utils.CryptoUtils;
import org.apache.pulsar.functions.utils.FunctionCommon;
import org.apache.pulsar.io.core.Sink;
import org.apache.pulsar.io.core.Source;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A function container implemented using java thread.
 */
@Slf4j
public class JavaInstanceRunnable implements AutoCloseable, Runnable {
    @Data
    public static class AsyncProcessingRequest {
        private final Record<?> record;
        private final Long startTime;
        private final CompletableFuture<Pair<Integer, JavaExecutionResult>> processResult;
    }

    private static final String srcRoot = "action-src";
    private static final String binRoot = "action-bin";
    private static final String errorHeader = "error:";

    private final InstanceConfig instanceConfig;
    private final String srcDir;
    private final String binDir;

    // input topic consumer & output topic producer
    private final ClientBuilder clientBuilder;
    private PulsarClientImpl client;
    private final PulsarAdmin pulsarAdmin;

    private LogAppender logAppender;

    // provide tables for storing states
    private final String stateStorageImplClass;
    private final String stateStorageServiceUrl;
    private StateStoreProvider stateStoreProvider;
    private StateManager stateManager;

    private String functionFile;
    private Server grpcServer;

    private final int parallelism;
    private final ArrayList<ProcessingUnit> units;
    private final LinkedBlockingQueue<AsyncProcessingRequest> pendingAsyncRequests;
    private final AtomicBoolean processingFlag = new AtomicBoolean(false);
    private boolean processingAttempt = false;

    private JavaInstance javaInstance;
    @Getter
    private Throwable deathException;

    // function stats
    private ComponentStatsManager stats;

    private Record<?> currentRecord;

    private Source source;
    private Sink sink;

    private final SecretsProvider secretsProvider;

    private FunctionCollectorRegistry collectorRegistry;
    private final String[] metricsLabels;

    private InstanceCache instanceCache;

    private final org.apache.pulsar.functions.proto.Function.FunctionDetails.ComponentType componentType;

    private final Map<String, String> properties;

    private final ClassLoader instanceClassLoader;
    private ClassLoader functionClassLoader;

    // a flog to determine if member variables have been initialized as part of setup().
    // used for out of band API calls like operations involving stats
    private transient boolean isInitialized = false;

    // a read write lock for stats operations
    private ReadWriteLock statsLock = new ReentrantReadWriteLock();

    private Map<String, String> compilers = new HashMap<String, String>() {{
        put("python", "pulsar-functions/compilers/python/compiler");
        put("nodejs", "pulsar-functions/compilers/nodejs/compiler");
        put("go", "pulsar-functions/compilers/golang/compiler");
    }};

    public JavaInstanceRunnable(InstanceConfig instanceConfig,
                                ClientBuilder clientBuilder,
                                String functionFile,
                                PulsarClient pulsarClient,
                                PulsarAdmin pulsarAdmin,
                                String stateStorageImplClass,
                                String stateStorageServiceUrl,
                                SecretsProvider secretsProvider,
                                FunctionCollectorRegistry collectorRegistry,
                                ClassLoader functionClassLoader) throws PulsarClientException {
        this.instanceConfig = instanceConfig;
        this.srcDir = String.format("%s/%s/%s", srcRoot, instanceConfig.getFunctionId(),
                instanceConfig.getFunctionVersion());
        this.binDir = String.format("%s/%s/%s", binRoot, instanceConfig.getFunctionId(),
                instanceConfig.getFunctionVersion());
        this.clientBuilder = clientBuilder;
        this.functionFile = functionFile;
        this.client = (PulsarClientImpl) pulsarClient;
        this.pulsarAdmin = pulsarAdmin;
        this.stateStorageImplClass = stateStorageImplClass;
        this.stateStorageServiceUrl = stateStorageServiceUrl;
        this.secretsProvider = secretsProvider;
        this.functionClassLoader = functionClassLoader;
        this.metricsLabels = new String[]{
                instanceConfig.getFunctionDetails().getTenant(),
                String.format("%s/%s", instanceConfig.getFunctionDetails().getTenant(),
                        instanceConfig.getFunctionDetails().getNamespace()),
                instanceConfig.getFunctionDetails().getName(),
                String.valueOf(instanceConfig.getInstanceId()),
                instanceConfig.getClusterName(),
                FunctionCommon.getFullyQualifiedName(instanceConfig.getFunctionDetails())
        };
        int parallelism1;
        try {
            parallelism1 = Integer.parseInt(System.getenv("UNIT_PARALLELISM"));
        } catch (NumberFormatException e) {
            parallelism1 = 3;
        }
        this.parallelism = parallelism1;
        this.units = new ArrayList<>(parallelism);
        this.pendingAsyncRequests = new LinkedBlockingQueue<>(this.instanceConfig.getMaxPendingAsyncRequests());

        this.componentType = InstanceUtils.calculateSubjectType(instanceConfig.getFunctionDetails());

        this.properties = InstanceUtils.getProperties(this.componentType,
                FunctionCommon.getFullyQualifiedName(instanceConfig.getFunctionDetails()),
                this.instanceConfig.getInstanceId());

        // Declare function local collector registry so that it will not clash with other function instances'
        // metrics collection especially in threaded mode
        // In process mode the JavaInstanceMain will declare a CollectorRegistry and pass it down
        this.collectorRegistry = collectorRegistry;

        this.instanceClassLoader = Thread.currentThread().getContextClassLoader();
    }

    /**
     * NOTE: this method should be called in the instance thread, in order to make class loading work.
     */
    synchronized private void setup() throws Exception {

        this.instanceCache = InstanceCache.getInstanceCache();

        if (this.collectorRegistry == null) {
            this.collectorRegistry = FunctionCollectorRegistry.getDefaultImplementation();
        }
        this.stats = ComponentStatsManager.getStatsManager(this.collectorRegistry, this.metricsLabels,
                this.instanceCache.getScheduledExecutorService(),
                this.componentType);

        // start the state table
        setupStateStore();

        ContextImpl contextImpl = setupContext();
        // start the output producer
        setupOutput(contextImpl);
        // start the input consumer
        setupInput(contextImpl);
        // use original way for java
        if (instanceConfig.getFunctionDetails().getRuntime()
                == org.apache.pulsar.functions.proto.Function.FunctionDetails.Runtime.JAVA) {
            // initialize the thread context
            ThreadContext.put("function", FunctionCommon.getFullyQualifiedName(instanceConfig.getFunctionDetails()));
            ThreadContext.put("functionname", instanceConfig.getFunctionDetails().getName());
            ThreadContext.put("instance", instanceConfig.getInstanceName());
            log.info("Starting Java Instance {} : \n Details = {}",
                    instanceConfig.getFunctionDetails().getName(), instanceConfig.getFunctionDetails());
            Object object;
            if (instanceConfig.getFunctionDetails().getClassName()
                    .equals(org.apache.pulsar.functions.windowing.WindowFunctionExecutor.class.getName())) {
                object = Reflections.createInstance(
                        instanceConfig.getFunctionDetails().getClassName(),
                        instanceClassLoader);
            } else {
                object = Reflections.createInstance(
                        instanceConfig.getFunctionDetails().getClassName(),
                        functionClassLoader);
            }

            if (!(object instanceof Function) && !(object instanceof java.util.function.Function)) {
                throw new RuntimeException("User class must either be Function or java.util.Function");
            }

            javaInstance = new JavaInstance(contextImpl, object, instanceConfig);
            try {
                Thread.currentThread().setContextClassLoader(functionClassLoader);
                javaInstance.initialize();
            } finally {
                Thread.currentThread().setContextClassLoader(instanceClassLoader);
            }
            // start any log topic handler
            setupLogHandler();
        } else if (StringUtils.isNotEmpty(functionFile)) {
            File destFile = new File(srcDir);
            // recreate src file
            if (destFile.exists()) {
                FileUtils.deleteDirectory(destFile);
            }
            if (!destFile.mkdirs()) {
                throw new RuntimeException("Failed to create dirs: " + destFile.getPath());
            }
            if (functionFile.endsWith(".zip")) {
                unzip(functionFile, destFile);
            } else {
                Path srcFile = Paths.get(functionFile);
                Path dst = Paths.get(destFile.getPath());
                Files.copy(srcFile, dst.resolve(srcFile.getFileName()), StandardCopyOption.REPLACE_EXISTING);
            }

            String compiler = compilers.get(instanceConfig.getFunctionDetails().getRuntime().name().toLowerCase());
            if (compiler == null) {
                throw new RuntimeException("Unsupported runtime: " + instanceConfig.getFunctionDetails().getRuntime());
            }
            String[] cmd = new String[]{
                    compiler,
                    instanceConfig.getFunctionDetails().getClassName(),
                    srcDir,
                    binDir
            };
            Process compileProcess = Runtime.getRuntime().exec(cmd);
            BufferedInputStream bis = new BufferedInputStream(
                    compileProcess.getInputStream());
            BufferedReader br = new BufferedReader(new InputStreamReader(bis, StandardCharsets.UTF_8));
            String line;
            while ((line = br.readLine()) != null) {
                System.out.println(line);
            }
            if (compileProcess.waitFor() != 0) {
                printStderr(compileProcess);
                throw new RuntimeException("Unable to compile : " + instanceConfig.getFunctionDetails().getRuntime());
            }
            bis.close();
            br.close();
            try {
                EpollEventLoopGroup group = new EpollEventLoopGroup();
                grpcServer = NettyServerBuilder.forAddress(new DomainSocketAddress(binDir + "/context.sock"))
                        .channelType(EpollServerDomainSocketChannel.class)
                        .workerEventLoopGroup(group)
                        .bossEventLoopGroup(group)
                        .addService(new ContextGrpcImpl(contextImpl))
                        .build()
                        .start();
            } catch (Exception e) {
                KQueueEventLoopGroup group = new KQueueEventLoopGroup();
                grpcServer =
                        NettyServerBuilder.forAddress(new DomainSocketAddress(binDir + "/context.sock"))
                                .channelType(KQueueServerDomainSocketChannel.class)
                                .workerEventLoopGroup(group)
                                .bossEventLoopGroup(group)
                                .addService(new ContextGrpcImpl(contextImpl))
                                .build()
                                .start();
            }

            // start executor
            String[] execCommand = new String[]{
                    binDir + "/exec",
                    "--tenant",
                    instanceConfig.getFunctionDetails().getTenant(),
                    "--namespace",
                    instanceConfig.getFunctionDetails().getNamespace(),
                    "--name",
                    instanceConfig.getFunctionDetails().getName(),
                    "--source",
                    JsonFormat.printer().print(instanceConfig.getFunctionDetails().getSource()),
                    "--sink",
                    JsonFormat.printer().print(instanceConfig.getFunctionDetails().getSink()),
                    "--instance_id",
                    String.valueOf(instanceConfig.getInstanceId()),
                    "--function_id",
                    instanceConfig.getFunctionId(),
                    "--function_version",
                    instanceConfig.getFunctionVersion(),
            };
            ArrayList<String> list = new ArrayList(Arrays.asList(execCommand));
            if (StringUtils.isNotEmpty(instanceConfig.getFunctionDetails().getUserConfig())) {
                list.add("--user_config");
                list.add(instanceConfig.getFunctionDetails().getUserConfig());
            }
            if (StringUtils.isNotEmpty(instanceConfig.getFunctionDetails().getSecretsMap())) {
                list.add("--secrets_map");
                list.add(instanceConfig.getFunctionDetails().getSecretsMap());
            }
            if (StringUtils.isNotEmpty(instanceConfig.getFunctionDetails().getSecretsProvider())) {
                list.add("--secrets_provider");
                list.add(instanceConfig.getFunctionDetails().getSecretsProvider());
            }
            if (StringUtils.isNotEmpty(instanceConfig.getFunctionDetails().getSecretsConfig())) {
                list.add("--secrets_provider_config");
                list.add(instanceConfig.getFunctionDetails().getSecretsConfig());
            }
            if (StringUtils.isNotEmpty(stateStorageServiceUrl)) {
                list.add("--state_storage_serviceurl");
                list.add(stateStorageServiceUrl);
            }
            if (StringUtils.isNotEmpty(instanceConfig.getFunctionDetails().getLogTopic())) {
                list.add("--log_topic");
                list.add(instanceConfig.getFunctionDetails().getLogTopic());
            }
            String[] commands = new String[list.size()];
            for (int i = 0; i < parallelism; i++) {
                log.info("Starting processing unit using command: {}", StringUtils.join(list, " "));
                ProcessingUnit unit = new ProcessingUnit(list.toArray(commands), i);
                units.add(unit);
            }
        }

        // to signal member variables are initialized
        isInitialized = true;
    }

    private void printStderr(Process process) throws IOException {
        BufferedInputStream es = new BufferedInputStream(process.getErrorStream());
        BufferedReader er = new BufferedReader(new InputStreamReader(es, StandardCharsets.UTF_8));
        String errLine;
        while ((errLine = er.readLine()) != null) {
            System.out.println(errLine);
        }
        es.close();
        er.close();
    }

    ContextImpl setupContext() throws PulsarClientException {
        Logger instanceLog = LoggerFactory.getILoggerFactory().getLogger(
                "function-" + instanceConfig.getFunctionDetails().getName());
        return new ContextImpl(instanceConfig, instanceLog, client, secretsProvider,
                collectorRegistry, metricsLabels, this.componentType, this.stats, stateManager,
                pulsarAdmin, clientBuilder);
    }

    public interface AsyncResultConsumer {
        void accept(Record record, JavaExecutionResult javaExecutionResult) throws Exception;
    }

    /**
     * The core logic that initialize the instance thread and executes the function.
     */
    @Override
    public void run() {
        try {
            setup();

            Thread currentThread = Thread.currentThread();
            Consumer<Throwable> asyncErrorHandler = throwable -> currentThread.interrupt();
            AsyncResultConsumer asyncResultConsumer =
                    (record, javaExecutionResult) -> handleResult(record, javaExecutionResult);

            Random rand = new Random();

            while (true) {
                currentRecord = readInput();

                // increment number of records received from source
                stats.incrTotalReceived();

                if (instanceConfig.getFunctionDetails().getProcessingGuarantees() == org.apache.pulsar.functions
                        .proto.Function.ProcessingGuarantees.ATMOST_ONCE) {
                    if (instanceConfig.getFunctionDetails().getAutoAck()) {
                        currentRecord.ack();
                    }
                }

                JavaExecutionResult result = null;

                // set last invocation time
                stats.setLastInvocation(System.currentTimeMillis());

                // process the message
                if (javaInstance != null) {
                    // start time for process latency stat
                    stats.processTimeStart();

                    addLogTopicHandler();
                    Thread.currentThread().setContextClassLoader(functionClassLoader);
                    result = javaInstance.handleMessage(
                            currentRecord,
                            currentRecord.getValue(),
                            asyncResultConsumer,
                            asyncErrorHandler);
                    Thread.currentThread().setContextClassLoader(instanceClassLoader);

                    // register end time
                    stats.processTimeEnd();
                } else {
                    // choose a processing unit randomly
                    ProcessingUnit unit = units.get(rand.nextInt(units.size()));
                    CompletableFuture<Pair<Integer, JavaExecutionResult>> processingResult = unit.run(currentRecord);
                    AsyncProcessingRequest request =
                            new AsyncProcessingRequest(currentRecord, System.nanoTime(), processingResult);
                    pendingAsyncRequests.put(request);

                    processingResult.whenCompleteAsync((res, cause) -> {
                        ProcessingUnit finishedUnit = units.get(res.getLeft());
                        try {
                            processAsyncResults(asyncResultConsumer);
                            // restart processing unit when there are some errors
                            if (res.getRight().getSystemException() != null) {
                                log.warn("Restarting processing unit: {}, error: {}", finishedUnit.index,
                                        res.getRight().getSystemException());
                                finishedUnit.recreate();
                            }
                        } catch (Exception e) {
                            log.error("Failed to put processing unit: {} back to the queue", finishedUnit.index, e);
                            asyncErrorHandler.accept(e);
                        }
                    });
                }

                removeLogTopicHandler();

                if (result != null) {
                    // process the synchronous results
                    handleResult(currentRecord, result);
                }
            }
        } catch (Throwable t) {
            log.error("[{}] Uncaught exception in Java Instance", FunctionCommon.getFullyQualifiedInstanceId(
                    instanceConfig.getFunctionDetails().getTenant(),
                    instanceConfig.getFunctionDetails().getNamespace(),
                    instanceConfig.getFunctionDetails().getName(),
                    instanceConfig.getInstanceId()), t);
            deathException = t;
            if (stats != null) {
                stats.incrSysExceptions(t);
            }
        } finally {
            log.info("Closing instance");
            close();
        }
    }

    private void processAsyncResults(JavaInstanceRunnable.AsyncResultConsumer resultConsumer) throws Exception {
        if (processingFlag.compareAndSet(false, true)) {
            AsyncProcessingRequest asyncResult = pendingAsyncRequests.peek();
            while (asyncResult != null && asyncResult.getProcessResult().isDone()) {
                pendingAsyncRequests.remove(asyncResult);
                JavaExecutionResult execResult;
                try {
                    execResult = asyncResult.getProcessResult().get().getRight();
                } catch (ExecutionException e) {
                    execResult = new JavaExecutionResult();
                    if (e.getCause() instanceof Exception) {
                        execResult.setUserException((Exception) e.getCause());
                    } else {
                        execResult.setUserException(new Exception(e.getCause()));
                    }
                }
                double endTimeMs = ((double) System.nanoTime() - asyncResult.startTime) / 1.0E6D;
                stats.processTimeDuration(endTimeMs);
                resultConsumer.accept(asyncResult.getRecord(), execResult);

                // peek the next result
                asyncResult = pendingAsyncRequests.peek();
            }
            processingFlag.set(false);
            if (processingAttempt) { // should retry
                processingAttempt = false;
                processAsyncResults(resultConsumer);
            }
        } else {
            processingAttempt = true;
        }
    }

    private void setupStateStore() throws Exception {
        this.stateManager = new InstanceStateManager();

        if (null == stateStorageServiceUrl) {
            stateStoreProvider = StateStoreProvider.NULL;
        } else {
            stateStoreProvider = getStateStoreProvider();
            Map<String, Object> stateStoreProviderConfig = new HashMap<>();
            stateStoreProviderConfig.put(BKStateStoreProviderImpl.STATE_STORAGE_SERVICE_URL, stateStorageServiceUrl);
            stateStoreProvider.init(stateStoreProviderConfig, instanceConfig.getFunctionDetails());

            StateStore store = stateStoreProvider.getStateStore(
                    instanceConfig.getFunctionDetails().getTenant(),
                    instanceConfig.getFunctionDetails().getNamespace(),
                    instanceConfig.getFunctionDetails().getName()
            );
            StateStoreContext context = new StateStoreContextImpl();
            store.init(context);

            stateManager.registerStore(store);
        }
    }

    private StateStoreProvider getStateStoreProvider() throws Exception {
        if (stateStorageImplClass == null) {
            return new BKStateStoreProviderImpl();
        } else {
            return (StateStoreProvider) Class.forName(stateStorageImplClass).getConstructor().newInstance();
        }
    }

    private void handleResult(Record srcRecord, JavaExecutionResult result) throws Exception {
        if (result.getUserException() != null) {
            Exception t = result.getUserException();
            log.warn("Encountered exception when processing message {}",
                    srcRecord, t);
            stats.incrUserExceptions(t);
            srcRecord.fail();
        } else if (result.getSystemException() != null) {
            Exception t = result.getSystemException();
            log.warn("Encountered exception when processing message {}",
                    srcRecord, t);
            stats.incrSysExceptions(t);
            srcRecord.fail();
        } else {
            if (result.getResult() != null) {
                sendOutputMessage(srcRecord, result.getResult());
            } else {
                if (instanceConfig.getFunctionDetails().getAutoAck()) {
                    // the function doesn't produce any result or the user doesn't want the result.
                    srcRecord.ack();
                }
            }
            // increment total successfully processed
            stats.incrTotalProcessedSuccessfully();
        }
    }

    private void sendOutputMessage(Record srcRecord, Object output) throws Exception {
        if (componentType == org.apache.pulsar.functions.proto.Function.FunctionDetails.ComponentType.SINK) {
            Thread.currentThread().setContextClassLoader(functionClassLoader);
        }
        try {
            this.sink.write(new SinkRecord<>(srcRecord, output));
        } catch (Exception e) {
            log.info("Encountered exception in sink write: ", e);
            stats.incrSinkExceptions(e);
            // fail the source record
            srcRecord.fail();
            throw e;
        } finally {
            Thread.currentThread().setContextClassLoader(instanceClassLoader);
        }
    }

    private Record readInput() throws Exception {
        Record record;
        if (componentType == org.apache.pulsar.functions.proto.Function.FunctionDetails.ComponentType.SOURCE) {
            Thread.currentThread().setContextClassLoader(functionClassLoader);
        }
        try {
            record = this.source.read();
        } catch (Exception e) {
            if (stats != null) {
                stats.incrSourceExceptions(e);
            }
            log.error("Encountered exception in source read", e);
            throw e;
        } finally {
            Thread.currentThread().setContextClassLoader(instanceClassLoader);
        }

        // check record is valid
        if (record == null) {
            throw new IllegalArgumentException("The record returned by the source cannot be null");
        } else if (record.getValue() == null) {
            throw new IllegalArgumentException("The value in the record returned by the source cannot be null");
        }
        return record;
    }

    /**
     * NOTE: this method is be synchronized because it is potentially called by two different places
     * one inside the run/finally clause and one inside the ThreadRuntime::stop.
     */
    @Override
    synchronized public void close() {

        isInitialized = false;

        if (stats != null) {
            stats.close();
            stats = null;
        }

        if (source != null) {
            if (componentType == org.apache.pulsar.functions.proto.Function.FunctionDetails.ComponentType.SOURCE) {
                Thread.currentThread().setContextClassLoader(functionClassLoader);
            }
            try {
                source.close();
            } catch (Throwable e) {
                log.error("Failed to close source {}", instanceConfig.getFunctionDetails().getSource().getClassName(),
                        e);
            } finally {
                Thread.currentThread().setContextClassLoader(instanceClassLoader);
            }
            source = null;
        }

        if (sink != null) {
            if (componentType == org.apache.pulsar.functions.proto.Function.FunctionDetails.ComponentType.SINK) {
                Thread.currentThread().setContextClassLoader(functionClassLoader);
            }
            try {
                sink.close();
            } catch (Throwable e) {
                log.error("Failed to close sink {}", instanceConfig.getFunctionDetails().getSource().getClassName(), e);
            } finally {
                Thread.currentThread().setContextClassLoader(instanceClassLoader);
            }
            sink = null;
        }

        if (null != javaInstance) {
            try {
                Thread.currentThread().setContextClassLoader(functionClassLoader);
                javaInstance.close();
            } finally {
                Thread.currentThread().setContextClassLoader(instanceClassLoader);
                javaInstance = null;
            }
        }

        if (null != stateManager) {
            stateManager.close();
        }

        if (null != stateStoreProvider) {
            stateStoreProvider.close();
        }

        instanceCache = null;

        if (logAppender != null) {
            removeLogTopicAppender(LoggerContext.getContext());
            removeLogTopicAppender(LoggerContext.getContext(false));
            logAppender.stop();
            logAppender = null;
        }

        if (grpcServer != null) {
            grpcServer.shutdown();
        }

        for (ProcessingUnit unit : units) {
            try {
                unit.close();
            } catch (IOException e) {
                log.error("Failed to close processing unit {}", unit.index, e);
            }
        }

    }

    public String getStatsAsString() throws IOException {
        if (isInitialized) {
            statsLock.readLock().lock();
            try {
                return stats.getStatsAsString();
            } finally {
                statsLock.readLock().unlock();
            }
        }
        return "";
    }

    public InstanceCommunication.MetricsData getAndResetMetrics() {
        if (isInitialized) {
            statsLock.writeLock().lock();
            try {
                InstanceCommunication.MetricsData metricsData = internalGetMetrics();
                internalResetMetrics();
                return metricsData;
            } finally {
                statsLock.writeLock().unlock();
            }
        }
        return InstanceCommunication.MetricsData.getDefaultInstance();
    }

    public InstanceCommunication.MetricsData getMetrics() {
        if (isInitialized) {
            statsLock.readLock().lock();
            try {
                return internalGetMetrics();
            } finally {
                statsLock.readLock().unlock();
            }
        }
        return InstanceCommunication.MetricsData.getDefaultInstance();
    }

    public void resetMetrics() {
        if (isInitialized) {
            statsLock.writeLock().lock();
            try {
                internalResetMetrics();
            } finally {
                statsLock.writeLock().unlock();
            }
        }
    }

    private InstanceCommunication.MetricsData internalGetMetrics() {
        InstanceCommunication.MetricsData.Builder bldr = createMetricsDataBuilder();

        Map<String, Double> userMetrics = javaInstance.getMetrics();
        if (userMetrics != null) {
            bldr.putAllUserMetrics(userMetrics);
        }
        return bldr.build();
    }

    private void internalResetMetrics() {
        stats.reset();
        javaInstance.resetMetrics();
    }

    private Builder createMetricsDataBuilder() {
        InstanceCommunication.MetricsData.Builder bldr = InstanceCommunication.MetricsData.newBuilder();
        if (stats != null) {
            bldr.setProcessedSuccessfullyTotal((long) stats.getTotalProcessedSuccessfully());
            bldr.setSystemExceptionsTotal((long) stats.getTotalSysExceptions());
            bldr.setUserExceptionsTotal((long) stats.getTotalUserExceptions());
            bldr.setReceivedTotal((long) stats.getTotalRecordsReceived());
            bldr.setAvgProcessLatency(stats.getAvgProcessLatency());
            bldr.setLastInvocation((long) stats.getLastInvocation());

            bldr.setProcessedSuccessfullyTotal1Min((long) stats.getTotalProcessedSuccessfully1min());
            bldr.setSystemExceptionsTotal1Min((long) stats.getTotalSysExceptions1min());
            bldr.setUserExceptionsTotal1Min((long) stats.getTotalUserExceptions1min());
            bldr.setReceivedTotal1Min((long) stats.getTotalRecordsReceived1min());
            bldr.setAvgProcessLatency1Min(stats.getAvgProcessLatency1min());
        }

        return bldr;
    }

    public InstanceCommunication.FunctionStatus.Builder getFunctionStatus() {
        InstanceCommunication.FunctionStatus.Builder functionStatusBuilder =
                InstanceCommunication.FunctionStatus.newBuilder();
        if (isInitialized) {
            statsLock.readLock().lock();
            try {
                functionStatusBuilder.setNumReceived((long) stats.getTotalRecordsReceived());
                functionStatusBuilder.setNumSuccessfullyProcessed((long) stats.getTotalProcessedSuccessfully());
                functionStatusBuilder.setNumUserExceptions((long) stats.getTotalUserExceptions());
                stats.getLatestUserExceptions().forEach(ex -> {
                    functionStatusBuilder.addLatestUserExceptions(ex);
                });
                functionStatusBuilder.setNumSystemExceptions((long) stats.getTotalSysExceptions());
                stats.getLatestSystemExceptions().forEach(ex -> {
                    functionStatusBuilder.addLatestSystemExceptions(ex);
                });
                stats.getLatestSourceExceptions().forEach(ex -> {
                    functionStatusBuilder.addLatestSourceExceptions(ex);
                });
                stats.getLatestSinkExceptions().forEach(ex -> {
                    functionStatusBuilder.addLatestSinkExceptions(ex);
                });
                functionStatusBuilder.setAverageLatency(stats.getAvgProcessLatency());
                functionStatusBuilder.setLastInvocationTime((long) stats.getLastInvocation());
            } finally {
                statsLock.readLock().unlock();
            }
        }
        return functionStatusBuilder;
    }

    private void setupLogHandler() {
        if (instanceConfig.getFunctionDetails().getLogTopic() != null &&
                !instanceConfig.getFunctionDetails().getLogTopic().isEmpty()) {
            // make sure Crc32cIntChecksum class is loaded before logging starts
            // to prevent "SSE4.2 CRC32C provider initialized" appearing in log topic
            new Crc32cIntChecksum();
            logAppender = new LogAppender(client, instanceConfig.getFunctionDetails().getLogTopic(),
                    FunctionCommon.getFullyQualifiedName(instanceConfig.getFunctionDetails()),
                    instanceConfig.getInstanceName());
            logAppender.start();
            setupLogTopicAppender(LoggerContext.getContext());
        }
    }

    private void addLogTopicHandler() {
        if (logAppender == null) {
            return;
        }
        setupLogTopicAppender(LoggerContext.getContext(false));
    }

    private void setupLogTopicAppender(LoggerContext context) {
        Configuration config = context.getConfiguration();
        config.addAppender(logAppender);
        for (final LoggerConfig loggerConfig : config.getLoggers().values()) {
            loggerConfig.addAppender(logAppender, null, null);
        }
        config.getRootLogger().addAppender(logAppender, null, null);
        context.updateLoggers();
    }

    private void removeLogTopicHandler() {
        if (logAppender == null) {
            return;
        }
        removeLogTopicAppender(LoggerContext.getContext(false));
    }

    private void removeLogTopicAppender(LoggerContext context) {
        Configuration config = context.getConfiguration();
        for (final LoggerConfig loggerConfig : config.getLoggers().values()) {
            loggerConfig.removeAppender(logAppender.getName());
        }
        config.getRootLogger().removeAppender(logAppender.getName());
        context.updateLoggers();
    }

    private void setupInput(ContextImpl contextImpl) throws Exception {

        SourceSpec sourceSpec = this.instanceConfig.getFunctionDetails().getSource();
        Object object;
        // If source classname is not set, we default pulsar source
        if (sourceSpec.getClassName().isEmpty()) {
            Map<String, ConsumerConfig> topicSchema = new TreeMap<>();
            sourceSpec.getInputSpecsMap().forEach((topic, conf) -> {
                ConsumerConfig consumerConfig =
                        ConsumerConfig.builder().isRegexPattern(conf.getIsRegexPattern()).build();

                if (instanceConfig.getFunctionDetails().getRuntime()
                        == org.apache.pulsar.functions.proto.Function.FunctionDetails.Runtime.JAVA) {
                    if (conf.getSchemaType() != null && !conf.getSchemaType().isEmpty()) {
                        consumerConfig.setSchemaType(conf.getSchemaType());
                    } else if (conf.getSerdeClassName() != null && !conf.getSerdeClassName().isEmpty()) {
                        consumerConfig.setSerdeClassName(conf.getSerdeClassName());
                    }
                    consumerConfig.setSchemaProperties(conf.getSchemaPropertiesMap());
                }
                consumerConfig.setConsumerProperties(conf.getConsumerPropertiesMap());
                if (conf.hasReceiverQueueSize()) {
                    consumerConfig.setReceiverQueueSize(conf.getReceiverQueueSize().getValue());
                }
                if (conf.hasCryptoSpec()) {
                    consumerConfig.setCryptoConfig(CryptoUtils.convertFromSpec(conf.getCryptoSpec()));
                }
                consumerConfig.setPoolMessages(conf.getPoolMessages());

                topicSchema.put(topic, consumerConfig);
            });

            sourceSpec.getTopicsToSerDeClassNameMap().forEach((topic, serde) -> {
                String finalSerde = serde;
                if (instanceConfig.getFunctionDetails().getRuntime()
                        != org.apache.pulsar.functions.proto.Function.FunctionDetails.Runtime.JAVA) {
                    finalSerde = byte[].class.getName();
                }
                topicSchema.put(topic,
                        ConsumerConfig.builder()
                                .serdeClassName(finalSerde)
                                .isRegexPattern(false)
                                .build());
            });

            if (!StringUtils.isEmpty(sourceSpec.getTopicsPattern())) {
                topicSchema.get(sourceSpec.getTopicsPattern()).setRegexPattern(true);
            }

            PulsarSourceConfig pulsarSourceConfig;
            // we can use a single consumer to read
            if (topicSchema.size() == 1) {
                SingleConsumerPulsarSourceConfig singleConsumerPulsarSourceConfig =
                        new SingleConsumerPulsarSourceConfig();
                Map.Entry<String, ConsumerConfig> entry = topicSchema.entrySet().iterator().next();
                singleConsumerPulsarSourceConfig.setTopic(entry.getKey());
                singleConsumerPulsarSourceConfig.setConsumerConfig(entry.getValue());
                pulsarSourceConfig = singleConsumerPulsarSourceConfig;
            } else {
                MultiConsumerPulsarSourceConfig multiConsumerPulsarSourceConfig = new MultiConsumerPulsarSourceConfig();
                multiConsumerPulsarSourceConfig.setTopicSchema(topicSchema);
                pulsarSourceConfig = multiConsumerPulsarSourceConfig;
            }

            pulsarSourceConfig.setSubscriptionName(
                    StringUtils.isNotBlank(sourceSpec.getSubscriptionName()) ? sourceSpec.getSubscriptionName()
                            : InstanceUtils.getDefaultSubscriptionName(instanceConfig.getFunctionDetails()));
            pulsarSourceConfig.setProcessingGuarantees(
                    FunctionConfig.ProcessingGuarantees.valueOf(
                            this.instanceConfig.getFunctionDetails().getProcessingGuarantees().name()));

            pulsarSourceConfig.setSubscriptionPosition(
                    convertFromFunctionDetailsSubscriptionPosition(sourceSpec.getSubscriptionPosition())
            );

            Preconditions.checkNotNull(contextImpl.getSubscriptionType());
            pulsarSourceConfig.setSubscriptionType(contextImpl.getSubscriptionType());

            if (instanceConfig.getFunctionDetails().getRuntime()
                    == org.apache.pulsar.functions.proto.Function.FunctionDetails.Runtime.JAVA) {
                pulsarSourceConfig.setTypeClassName(sourceSpec.getTypeClassName());
            } else {
                pulsarSourceConfig.setTypeClassName(byte[].class.getName());
            }

            if (sourceSpec.getTimeoutMs() > 0) {
                pulsarSourceConfig.setTimeoutMs(sourceSpec.getTimeoutMs());
            }
            if (sourceSpec.getNegativeAckRedeliveryDelayMs() > 0) {
                pulsarSourceConfig.setNegativeAckRedeliveryDelayMs(sourceSpec.getNegativeAckRedeliveryDelayMs());
            }

            if (this.instanceConfig.getFunctionDetails().hasRetryDetails()) {
                pulsarSourceConfig.setMaxMessageRetries(
                        this.instanceConfig.getFunctionDetails().getRetryDetails().getMaxMessageRetries());
                pulsarSourceConfig.setDeadLetterTopic(
                        this.instanceConfig.getFunctionDetails().getRetryDetails().getDeadLetterTopic());
            }

            // Use SingleConsumerPulsarSource if possible because it will have higher performance since it is not a
            // push source
            // that require messages to be put into an immediate queue
            if (pulsarSourceConfig instanceof SingleConsumerPulsarSourceConfig) {
                object = new SingleConsumerPulsarSource(this.client,
                        (SingleConsumerPulsarSourceConfig) pulsarSourceConfig, this.properties,
                        this.functionClassLoader);
            } else {
                object =
                        new MultiConsumerPulsarSource(this.client, (MultiConsumerPulsarSourceConfig) pulsarSourceConfig,
                                this.properties, this.functionClassLoader);
            }
        } else {

            // check if source is a batch source
            if (sourceSpec.getClassName().equals(BatchSourceExecutor.class.getName())) {
                object = Reflections.createInstance(
                        sourceSpec.getClassName(),
                        this.instanceClassLoader);
            } else {
                object = Reflections.createInstance(
                        sourceSpec.getClassName(),
                        this.functionClassLoader);
            }
        }

        Class<?>[] typeArgs;
        if (object instanceof Source) {
            typeArgs = TypeResolver.resolveRawArguments(Source.class, object.getClass());
            assert typeArgs.length > 0;
        } else {
            throw new RuntimeException("Source does not implement correct interface");
        }
        this.source = (Source<?>) object;

        if (componentType == org.apache.pulsar.functions.proto.Function.FunctionDetails.ComponentType.SOURCE) {
            Thread.currentThread().setContextClassLoader(this.functionClassLoader);
        }
        try {
            if (sourceSpec.getConfigs().isEmpty()) {
                this.source.open(new HashMap<>(), contextImpl);
            } else {
                this.source.open(ObjectMapperFactory.getThreadLocal().readValue(sourceSpec.getConfigs(),
                        new TypeReference<Map<String, Object>>() {
                        }), contextImpl);
            }
            if (this.source instanceof PulsarSource) {
                contextImpl.setInputConsumers(((PulsarSource) this.source).getInputConsumers());
            }
        } catch (Exception e) {
            log.error("Source open produced uncaught exception: ", e);
            throw e;
        } finally {
            Thread.currentThread().setContextClassLoader(this.instanceClassLoader);
        }
    }

    private void setupOutput(ContextImpl contextImpl) throws Exception {

        SinkSpec sinkSpec = this.instanceConfig.getFunctionDetails().getSink();
        Object object;
        // If sink classname is not set, we default pulsar sink
        if (sinkSpec.getClassName().isEmpty()) {
            if (StringUtils.isEmpty(sinkSpec.getTopic())) {
                object = PulsarSinkDisable.INSTANCE;
            } else {
                PulsarSinkConfig pulsarSinkConfig = new PulsarSinkConfig();
                pulsarSinkConfig.setProcessingGuarantees(FunctionConfig.ProcessingGuarantees.valueOf(
                        this.instanceConfig.getFunctionDetails().getProcessingGuarantees().name()));
                pulsarSinkConfig.setTopic(sinkSpec.getTopic());
                pulsarSinkConfig.setForwardSourceMessageProperty(
                        this.instanceConfig.getFunctionDetails().getSink().getForwardSourceMessageProperty());

                // no schema and serde for other runtime(default to bytes)
                if (instanceConfig.getFunctionDetails().getRuntime()
                        == org.apache.pulsar.functions.proto.Function.FunctionDetails.Runtime.JAVA) {
                    if (!StringUtils.isEmpty(sinkSpec.getSchemaType())) {
                        pulsarSinkConfig.setSchemaType(sinkSpec.getSchemaType());
                    } else if (!StringUtils.isEmpty(sinkSpec.getSerDeClassName())) {
                        pulsarSinkConfig.setSerdeClassName(sinkSpec.getSerDeClassName());
                    }

                    pulsarSinkConfig.setTypeClassName(sinkSpec.getTypeClassName());
                    pulsarSinkConfig.setSchemaProperties(sinkSpec.getSchemaPropertiesMap());
                } else {
                    pulsarSinkConfig.setTypeClassName(byte[].class.getName());
                }

                if (this.instanceConfig.getFunctionDetails().getSink().getProducerSpec() != null) {
                    org.apache.pulsar.functions.proto.Function.ProducerSpec conf =
                            this.instanceConfig.getFunctionDetails().getSink().getProducerSpec();
                    ProducerConfig.ProducerConfigBuilder builder = ProducerConfig.builder()
                            .maxPendingMessages(conf.getMaxPendingMessages())
                            .maxPendingMessagesAcrossPartitions(conf.getMaxPendingMessagesAcrossPartitions())
                            .batchBuilder(conf.getBatchBuilder())
                            .useThreadLocalProducers(conf.getUseThreadLocalProducers())
                            .cryptoConfig(CryptoUtils.convertFromSpec(conf.getCryptoSpec()));
                    pulsarSinkConfig.setProducerConfig(builder.build());
                }

                object = new PulsarSink(this.client, pulsarSinkConfig, this.properties, this.stats,
                        this.functionClassLoader);
            }
        } else {
            object = Reflections.createInstance(
                    sinkSpec.getClassName(),
                    this.functionClassLoader);
        }

        if (object instanceof Sink) {
            this.sink = (Sink) object;
        } else {
            throw new RuntimeException("Sink does not implement correct interface");
        }

        if (componentType == org.apache.pulsar.functions.proto.Function.FunctionDetails.ComponentType.SINK) {
            Thread.currentThread().setContextClassLoader(this.functionClassLoader);
        }
        try {
            if (sinkSpec.getConfigs().isEmpty()) {
                if (log.isDebugEnabled()) {
                    log.debug("Opening Sink with empty hashmap with contextImpl: {} ", contextImpl.toString());
                }
                this.sink.open(new HashMap<>(), contextImpl);
            } else {
                if (log.isDebugEnabled()) {
                    log.debug("Opening Sink with SinkSpec {} and contextImpl: {} ", sinkSpec.toString(),
                            contextImpl.toString());
                }
                this.sink.open(ObjectMapperFactory.getThreadLocal().readValue(sinkSpec.getConfigs(),
                        new TypeReference<Map<String, Object>>() {
                        }), contextImpl);
            }
        } catch (Exception e) {
            log.error("Sink open produced uncaught exception: ", e);
            throw e;
        } finally {
            Thread.currentThread().setContextClassLoader(this.instanceClassLoader);
        }
    }

    private String byteToHex(byte num) {
        char[] hexDigits = new char[2];
        hexDigits[0] = Character.forDigit((num >> 4) & 0xF, 16);
        hexDigits[1] = Character.forDigit((num & 0xF), 16);
        return new String(hexDigits);
    }

    private String byteArrayToHexString(byte[] byteArray) {
        StringBuilder hexStringBuffer = new StringBuilder();
        for (int i = 0; i < byteArray.length; i++) {
            hexStringBuffer.append(byteToHex(byteArray[i]));
        }
        return hexStringBuffer.toString();
    }

    private byte[] hexStringToByteArray(String s) {
        int len = s.length();
        byte[] data = new byte[len / 2];
        for (int i = 0; i < len; i += 2) {
            data[i / 2] = (byte) ((Character.digit(s.charAt(i), 16) << 4)
                    + Character.digit(s.charAt(i + 1), 16));
        }
        return data;
    }

    private void unzip(String fileZip, File destDir) throws IOException {
        byte[] buffer = new byte[1024];
        ZipInputStream zis = new ZipInputStream(new FileInputStream(fileZip));
        ZipEntry zipEntry = zis.getNextEntry();
        while (zipEntry != null) {
            File newFile = newFile(destDir, zipEntry);
            if (zipEntry.isDirectory()) {
                if (!newFile.isDirectory() && !newFile.mkdirs()) {
                    throw new IOException("Failed to create directory " + newFile);
                }
            } else {
                // fix for Windows-created archives
                File parent = newFile.getParentFile();
                if (!parent.isDirectory() && !parent.mkdirs()) {
                    throw new IOException("Failed to create directory " + parent);
                }

                // write file content
                FileOutputStream fos = new FileOutputStream(newFile);
                int len;
                while (true) {
                    try {
                        len = zis.read(buffer);
                        if (len <= 0) {
                            break;
                        }
                        fos.write(buffer, 0, len);
                    } catch (IOException e) {
                        break;
                    }
                }
                fos.close();
            }
            zipEntry = zis.getNextEntry();
        }

        zis.closeEntry();
        zis.close();
    }

    public static File newFile(File destinationDir, ZipEntry zipEntry) throws IOException {
        File destFile = new File(destinationDir, zipEntry.getName());

        String destDirPath = destinationDir.getCanonicalPath();
        String destFilePath = destFile.getCanonicalPath();

        if (!destFilePath.startsWith(destDirPath + File.separator)) {
            throw new IOException("Entry is outside of the target dir: " + zipEntry.getName());
        }

        return destFile;
    }

    private class ProcessingUnit {
        private BufferedReader functionOut;
        private BufferedOutputStream functionIn;
        private Process execProcess;
        private String[] commands;
        private int index;

        public ProcessingUnit(String[] commands, int index) throws IOException {
            this.commands = commands;
            this.index = index;
            this.startProcess();
        }

        private void startProcess() throws IOException {
            this.execProcess = Runtime.getRuntime().exec(commands);
            this.functionOut = new BufferedReader(
                    new InputStreamReader(new BufferedInputStream(execProcess.getInputStream()),
                            StandardCharsets.UTF_8));
            this.functionIn = new BufferedOutputStream(execProcess.getOutputStream());
        }


        public void recreate() throws IOException {
            execProcess.destroyForcibly();
            this.close();

            this.startProcess();
        }

        public synchronized CompletableFuture<Pair<Integer, JavaExecutionResult>> run(Record<?> record) {
            return CompletableFuture.supplyAsync(() -> {
                JavaExecutionResult executionResult = new JavaExecutionResult();
                if (execProcess != null && !execProcess.isAlive()) {
                    try {
                        printStderr(execProcess);
                    } catch (IOException ignored) {
                    }
                    executionResult.setSystemException(
                            new RuntimeException("Processing unit is exited with code: " + execProcess.exitValue()));
                    return Pair.of(index, executionResult);
                }

                if (record.getMessage().isPresent()) {
                    Message<?> msg = record.getMessage().get();
                    try {
                        // write the topic name too
                        ByteBuffer buffer = ByteBuffer.allocate(2 + msg.getTopicName().length() + msg.getData().length);
                        // topic name should be shorter than 256
                        buffer.put((byte)msg.getTopicName().length());
                        buffer.put(msg.getTopicName().getBytes(StandardCharsets.UTF_8));
                        buffer.put(msg.getData());
                        buffer.put("\n".getBytes(StandardCharsets.UTF_8));
                        functionIn.write(buffer.array());
                        functionIn.flush();
                    } catch (IOException e) {
                        executionResult.setSystemException(e);
                        return Pair.of(index, executionResult);
                    }
                } else {
                    // empty message return an empty result
                    return Pair.of(index, executionResult);
                }
                String output = null;
                try {
                    output = functionOut.readLine();
                } catch (IOException e) {
                    executionResult.setSystemException(e);
                    return Pair.of(index, executionResult);
                }

                if (output == null || output.startsWith(errorHeader)) {
                    String errorMsg = "empty output";
                    if (output != null) {
                        errorMsg = output.replaceFirst(errorHeader, "");
                    }
                    log.error("Failed to process message: {}, error: {}", currentRecord.getRecordSequence(), errorMsg);
                    executionResult.setUserException(new Exception(errorMsg));
                } else {
                    executionResult.setResult(output.getBytes(StandardCharsets.UTF_8));
                }
                return Pair.of(index, executionResult);
            });
        }

        public void close() throws IOException {
            this.functionIn.close();
            this.functionOut.close();
        }
    }

    class ContextGrpcImpl extends ContextGrpc.ContextImplBase {
        private ContextImpl contextImpl;

        public ContextGrpcImpl(ContextImpl contextImpl) {
            this.contextImpl = contextImpl;
        }

        @Override
        public void log(InstanceContext.LogMessage request, StreamObserver<Empty> responseObserver) {
            switch (request.getLogLevel()) {
                case "trace":
                    log.trace(request.getMsg());
                    break;
                case "debug":
                    log.debug(request.getMsg());
                    break;
                case "warn":
                case "warning":
                    log.warn(request.getMsg());
                    break;
                case "error":
                    log.error(request.getMsg());
                    break;
                default:
                    log.info(request.getMsg());
                    break;
            }
            responseObserver.onNext(com.google.protobuf.Empty.getDefaultInstance());
            responseObserver.onCompleted();
        }

        @Override
        public void publish(InstanceContext.PulsarMessage request,
                            StreamObserver<Empty> responseObserver) {
            contextImpl.publish(request.getTopic(), request.getPayload().toByteArray()).thenApply(messageId -> {
                responseObserver.onNext(com.google.protobuf.Empty.getDefaultInstance());
                responseObserver.onCompleted();
                return null;
            }).exceptionally(ex -> {
                responseObserver.onError(ex);
                return null;
            });
        }

        @Override
        public void currentRecord(Empty request, StreamObserver<InstanceContext.Record> responseObserver) {
            contextImpl.getCurrentRecord().getMessage().map(msg -> {
                InstanceContext.Record.Builder builder = InstanceContext.Record.newBuilder();
                builder.setPayload(ByteString.copyFrom(msg.getData()));
                responseObserver.onNext(builder.build());
                responseObserver.onCompleted();
                return null;
            }).orElseGet(() -> {
                responseObserver.onError(new RuntimeException("No current record"));
                return null;
            });
        }

        @Override
        public void recordMetrics(InstanceContext.MetricData request, StreamObserver<Empty> responseObserver) {
            contextImpl.recordMetric(request.getMetricName(), request.getValue());
            responseObserver.onNext(com.google.protobuf.Empty.getDefaultInstance());
            responseObserver.onCompleted();
        }

        @Override
        public void seek(InstanceContext.Partition request, StreamObserver<Empty> responseObserver) {
            try {
                contextImpl.seek(request.getTopicName(), request.getPartitionIndex(),
                        MessageId.fromByteArray(request.getMessageId().toByteArray()));
            } catch (IOException e) {
                log.error("Exception in JavaInstance doing seek", e);
                throw new RuntimeException(e);
            }
        }

        @Override
        public void pause(InstanceContext.Partition request, StreamObserver<Empty> responseObserver) {
            try {
                contextImpl.pause(request.getTopicName(), request.getPartitionIndex());
            } catch (PulsarClientException e) {
                log.error("Exception in JavaInstance doing pause", e);
                throw new RuntimeException(e);
            }
        }

        @Override
        public void resume(InstanceContext.Partition request, StreamObserver<Empty> responseObserver) {
            try {
                contextImpl.resume(request.getTopicName(), request.getPartitionIndex());
            } catch (PulsarClientException e) {
                log.error("Exception in JavaInstance doing resume", e);
                throw new RuntimeException(e);
            }
        }
    }
}
