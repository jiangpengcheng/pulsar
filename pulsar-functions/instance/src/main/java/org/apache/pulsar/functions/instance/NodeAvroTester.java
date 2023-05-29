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

import java.util.Optional;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.ClientBuilder;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.functions.proto.Function;

public class NodeAvroTester {
    @SuppressWarnings({"checkstyle:RegexpSingleline", "checkstyle:LineLength"})
    public static void main(String[] args) throws PulsarClientException {
        InstanceConfig instanceConfig = new InstanceConfig();
        instanceConfig.setInstanceId(0);
        instanceConfig.setClusterName("standalone");
        instanceConfig.setFunctionId("test-node-avro");
        instanceConfig.setFunctionVersion("0.0.1");

        instanceConfig.setMaxBufferedTuples(1000);
        instanceConfig.setPort(9090);
        instanceConfig.setMetricsPort(9091);

        Function.FunctionDetails.Builder functionDetailsBuilder = Function.FunctionDetails.newBuilder();
        //functionDetailsBuilder.setLogTopic("persistent://public/default/test-node-package-serde-log");

        Function.SourceSpec.Builder sourceSpecBuilder = Function.SourceSpec.newBuilder();
        sourceSpecBuilder.putInputSpecs("persistent://public/default/test-node-package-avro-source", Function.ConsumerSpec.newBuilder().setIsRegexPattern(false).setSchemaType("avro").build());
        sourceSpecBuilder.setSubscriptionType(Function.SubscriptionType.SHARED);
        sourceSpecBuilder.setSubscriptionName("test-sub");
        sourceSpecBuilder.setSubscriptionPosition(Function.SubscriptionPosition.LATEST);
        sourceSpecBuilder.setTypeClassName("checkavro.definitions");
        functionDetailsBuilder.setSource(sourceSpecBuilder.build());

        Function.SinkSpec.Builder sinkSpecBuilder = Function.SinkSpec.newBuilder();
        sinkSpecBuilder.setTopic("persistent://public/default/test-node-package-avro-sink");
        sinkSpecBuilder.setSchemaType("avro");
        sinkSpecBuilder.setTypeClassName("checkavro.definitions");
        functionDetailsBuilder.setSink(sinkSpecBuilder.build());

        functionDetailsBuilder.setTenant("public");
        functionDetailsBuilder.setNamespace("default");
        functionDetailsBuilder.setName("test-node-avro");

        functionDetailsBuilder.setRuntime(Function.FunctionDetails.Runtime.NODEJS);
        functionDetailsBuilder.setClassName("checkavro");

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
                "checkavro.js",
                pulsarClient,
                null,
                null, null, null, null, Thread.currentThread().getContextClassLoader());
        javaInstanceRunnable.run();
    }
}
