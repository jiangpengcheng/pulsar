package org.apache.pulsar.functions.instance;

import java.util.Optional;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.ClientBuilder;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.functions.proto.Function;

public class PythonAvroTester {
    public static void main(String[] args) throws PulsarClientException {
        InstanceConfig instanceConfig = new InstanceConfig();
        instanceConfig.setInstanceId(0);
        instanceConfig.setClusterName("standalone");
        instanceConfig.setFunctionId("test-python-avro");
        instanceConfig.setFunctionVersion("0.0.1");

        instanceConfig.setMaxBufferedTuples(1000);
        instanceConfig.setPort(9090);
        instanceConfig.setMetricsPort(9091);

        Function.FunctionDetails.Builder functionDetailsBuilder = Function.FunctionDetails.newBuilder();

        Function.SourceSpec.Builder sourceSpecBuilder = Function.SourceSpec.newBuilder();
        sourceSpecBuilder.putInputSpecs("persistent://public/default/test-py-package-avro-source", Function.ConsumerSpec.newBuilder().setIsRegexPattern(false).setSchemaType("avro").build());
        sourceSpecBuilder.setSubscriptionType(Function.SubscriptionType.SHARED);
        sourceSpecBuilder.setSubscriptionName("test-avro-sub");
        sourceSpecBuilder.setSubscriptionPosition(Function.SubscriptionPosition.LATEST);
        sourceSpecBuilder.setTypeClassName("checkavro.Student");
        functionDetailsBuilder.setSource(sourceSpecBuilder.build());

        Function.SinkSpec.Builder sinkSpecBuilder = Function.SinkSpec.newBuilder();
        sinkSpecBuilder.setTopic("persistent://public/default/test-py-package-avro-sink");
        sinkSpecBuilder.setSchemaType("avro");
        sinkSpecBuilder.setTypeClassName("checkavro.Student");
        functionDetailsBuilder.setSink(sinkSpecBuilder.build());

        functionDetailsBuilder.setTenant("public");
        functionDetailsBuilder.setNamespace("default");
        functionDetailsBuilder.setName("test-py-avro-package");

        functionDetailsBuilder.setRuntime(Function.FunctionDetails.Runtime.PYTHON);
        functionDetailsBuilder.setClassName("checkavro");

        functionDetailsBuilder.setComponentType(Function.FunctionDetails.ComponentType.FUNCTION);
        instanceConfig.setFunctionDetails(functionDetailsBuilder.build());

        String pulsarServiceUrl = "pulsar://localhost:6650";
        ClientBuilder clientBuilder = InstanceUtils
                .createPulsarClientBuilder(pulsarServiceUrl, null, Optional.empty());
        PulsarClient pulsarClient;
        pulsarClient = clientBuilder.build();
        PulsarAdmin pulsarAdmin = null;

        JavaInstanceRunnable javaInstanceRunnable = new JavaInstanceRunnable(
                instanceConfig,
                clientBuilder,
                "checkavro.py",
                pulsarClient,
                null,
                null, null, null, null, Thread.currentThread().getContextClassLoader());
        javaInstanceRunnable.run();
    }
}
