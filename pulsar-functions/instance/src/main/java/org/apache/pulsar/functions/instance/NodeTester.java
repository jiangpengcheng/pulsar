package org.apache.pulsar.functions.instance;

import java.util.Optional;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.ClientBuilder;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.functions.proto.Function;

public class NodeTester {
    @SuppressWarnings({"checkstyle:RegexpSingleline", "checkstyle:LineLength"})
    public static void main(String[] args) throws PulsarClientException {
        InstanceConfig instanceConfig = new InstanceConfig();
        instanceConfig.setInstanceId(0);
        instanceConfig.setClusterName("standalone");
        instanceConfig.setFunctionId("test-0");
        instanceConfig.setTransformFunctionId("trans-0");
        instanceConfig.setFunctionVersion("0.0.1");

        instanceConfig.setMaxBufferedTuples(1000);
        instanceConfig.setPort(9090);
        instanceConfig.setMetricsPort(9091);

        Function.FunctionDetails.Builder functionDetailsBuilder = Function.FunctionDetails.newBuilder();

        Function.SourceSpec.Builder sourceSpecBuilder = Function.SourceSpec.newBuilder();
        sourceSpecBuilder.putInputSpecs("persistent://public/default/test-node-package-source", Function.ConsumerSpec.newBuilder().setIsRegexPattern(false).build());
        sourceSpecBuilder.setSubscriptionType(Function.SubscriptionType.SHARED);
        sourceSpecBuilder.setSubscriptionName("test-sub");
        sourceSpecBuilder.setSubscriptionPosition(Function.SubscriptionPosition.LATEST);
        sourceSpecBuilder.setTypeClassName("java.lang.String");
        functionDetailsBuilder.setSource(sourceSpecBuilder.build());

        Function.SinkSpec.Builder sinkSpecBuilder = Function.SinkSpec.newBuilder();
        sinkSpecBuilder.setTopic("persistent://public/default/test-node-package-sink");
        sinkSpecBuilder.setTypeClassName("java.lang.String");
        functionDetailsBuilder.setSink(sinkSpecBuilder.build());

        functionDetailsBuilder.setTenant("public");
        functionDetailsBuilder.setNamespace("default");
        functionDetailsBuilder.setName("test-node-package");

        functionDetailsBuilder.setRuntime(Function.FunctionDetails.Runtime.NODEJS);
        functionDetailsBuilder.setClassName("exclamation");

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
                "exclamation.js",
                pulsarClient,
                null,
                null, null, null, null, Thread.currentThread().getContextClassLoader(), null);
        javaInstanceRunnable.run();
    }
}
