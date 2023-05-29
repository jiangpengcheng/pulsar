package org.apache.pulsar.functions.instance;

import java.util.Optional;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.ClientBuilder;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.functions.proto.Function;

public class PythonTester {
    public static void main(String[] args) throws PulsarClientException {
        InstanceConfig instanceConfig = new InstanceConfig();
        instanceConfig.setInstanceId(0);
        instanceConfig.setClusterName("standalone");
        instanceConfig.setFunctionId("test-python");
        instanceConfig.setFunctionVersion("0.0.1");

        instanceConfig.setMaxBufferedTuples(1000);
        instanceConfig.setPort(9090);
        instanceConfig.setMetricsPort(9091);

        Function.FunctionDetails.Builder functionDetailsBuilder = Function.FunctionDetails.newBuilder();
        functionDetailsBuilder.setLogTopic("persistent://public/default/test-py-package-serde-log");

        Function.SourceSpec.Builder sourceSpecBuilder = Function.SourceSpec.newBuilder();
        sourceSpecBuilder.putInputSpecs("persistent://public/default/test-py-package-serde-source", Function.ConsumerSpec.newBuilder().setIsRegexPattern(false).build());
        sourceSpecBuilder.setSubscriptionType(Function.SubscriptionType.SHARED);
        sourceSpecBuilder.setSubscriptionName("test-sub");
        sourceSpecBuilder.setSubscriptionPosition(Function.SubscriptionPosition.LATEST);
        sourceSpecBuilder.setTypeClassName("String");
        functionDetailsBuilder.setSource(sourceSpecBuilder.build());

        Function.SinkSpec.Builder sinkSpecBuilder = Function.SinkSpec.newBuilder();
        sinkSpecBuilder.setTopic("persistent://public/default/test-py-package-serde-sink");
        sinkSpecBuilder.setTypeClassName("String");
        functionDetailsBuilder.setSink(sinkSpecBuilder.build());

        functionDetailsBuilder.setTenant("public");
        functionDetailsBuilder.setNamespace("default");
        functionDetailsBuilder.setName("test-py-package");

        functionDetailsBuilder.setRuntime(Function.FunctionDetails.Runtime.PYTHON);
        functionDetailsBuilder.setClassName("exclamation.ExclamationFunction");

        functionDetailsBuilder.setComponentType(Function.FunctionDetails.ComponentType.FUNCTION);
        instanceConfig.setFunctionDetails(functionDetailsBuilder.build());

        String pulsarServiceUrl = "pulsar://localhost:6650";
        ClientBuilder clientBuilder = InstanceUtils
                .createPulsarClientBuilder(pulsarServiceUrl, null, Optional.empty());
        PulsarClient pulsarClient = clientBuilder.build();
        PulsarAdmin pulsarAdmin = null;

        JavaInstanceRunnable javaInstanceRunnable = new JavaInstanceRunnable(
                instanceConfig,
                clientBuilder,
                "exclamation.py",
                pulsarClient,
                null,
                "io.streamnative.pulsar.state.OXIAStateStoreProviderImpl", "localhost:6648", null, null, Thread.currentThread().getContextClassLoader());
        javaInstanceRunnable.run();
    }
}
