package org.apache.pulsar.functions.instance;

import java.util.Optional;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.ClientBuilder;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.functions.proto.Function;

public class PythonZIPTester {
    public static void main(String[] args) throws PulsarClientException {
        InstanceConfig instanceConfig = new InstanceConfig();
        instanceConfig.setInstanceId(0);
        instanceConfig.setClusterName("standalone");
        instanceConfig.setFunctionId("test-py-zip");
        instanceConfig.setFunctionVersion("0.0.1");

        instanceConfig.setMaxBufferedTuples(1000);
        instanceConfig.setPort(9090);
        instanceConfig.setMetricsPort(9091);

        Function.FunctionDetails.Builder functionDetailsBuilder = Function.FunctionDetails.newBuilder();

        Function.SourceSpec.Builder sourceSpecBuilder = Function.SourceSpec.newBuilder();
        sourceSpecBuilder.putInputSpecs("persistent://public/default/test-py-package-zip-source", Function.ConsumerSpec.newBuilder().setIsRegexPattern(false).build());
        sourceSpecBuilder.setSubscriptionType(Function.SubscriptionType.SHARED);
        sourceSpecBuilder.setSubscriptionName("test-sub");
        sourceSpecBuilder.setSubscriptionPosition(Function.SubscriptionPosition.LATEST);
        sourceSpecBuilder.setTypeClassName("String");
        functionDetailsBuilder.setSource(sourceSpecBuilder.build());

        Function.SinkSpec.Builder sinkSpecBuilder = Function.SinkSpec.newBuilder();
        sinkSpecBuilder.setTopic("persistent://public/default/test-py-package-zip-sink");
        functionDetailsBuilder.setSink(sinkSpecBuilder.build());

        functionDetailsBuilder.setTenant("public");
        functionDetailsBuilder.setNamespace("default");
        functionDetailsBuilder.setName("test-py-zip");

        functionDetailsBuilder.setRuntime(Function.FunctionDetails.Runtime.PYTHON);
        functionDetailsBuilder.setClassName("myhttp");

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
                "httppy.zip",
                pulsarClient,
                null,
                null, null, null, null, Thread.currentThread().getContextClassLoader());
        javaInstanceRunnable.run();
    }
}
