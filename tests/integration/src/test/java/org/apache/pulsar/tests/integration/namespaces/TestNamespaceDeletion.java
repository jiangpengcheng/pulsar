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
package org.apache.pulsar.tests.integration.namespaces;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.tests.integration.docker.ContainerExecException;
import org.apache.pulsar.tests.integration.docker.ContainerExecResult;
import org.apache.pulsar.tests.integration.suites.PulsarTestSuite;
import org.apache.pulsar.tests.integration.topologies.FunctionRuntimeType;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Test cases for namespace deletion.
 */
@Slf4j
public class TestNamespaceDeletion extends PulsarTestSuite {
    private final String tenant = "test-tenant-" + randomName(4);
    public static final String JAR = "/pulsar/examples/java-test-functions.jar";

    @Override
    public void setupCluster() throws Exception {
        brokerEnvs.put("forceDeleteNamespaceAllowed", "true");
        brokerEnvs.put("forceDeleteTenantAllowed", "true");
        brokerEnvs.put("functionsWorkerEnabled", "true");
        super.setupCluster("");
        pulsarCluster.setupFunctionWorkers(randomName(5), FunctionRuntimeType.PROCESS, 2);

        pulsarCluster.runAdminCommandOnAnyBroker( "tenants",
                "create", "--allowed-clusters", pulsarCluster.getClusterName(), tenant);
    }

    @Override
    public void tearDownCluster() throws Exception {
        try {
            pulsarCluster.runAdminCommandOnAnyBroker("tenants", "delete", "--force", tenant);
        } catch (Exception e) {
            log.warn("Failed to delete tenant {}: {}", tenant, e.getMessage());
        } finally {
            super.tearDownCluster();
        }
    }

    @Test
    public void testNamespaceDeletion() throws Exception {
        final String namespace = tenant + "/ns-empty";

        pulsarCluster.runAdminCommandOnAnyBroker("namespaces", "create", "-c",
                pulsarCluster.getClusterName(), namespace);

        // empty namespace should be deleted without `--force` argument
        ContainerExecResult result = pulsarCluster.runAdminCommandOnAnyBroker("namespaces", "delete",
                namespace);
        assertEquals(0, result.getExitCode());

        // the namespace should be deleted
        try {
            pulsarCluster.runAdminCommandOnAnyBroker("namespaces", "get-clusters", namespace);
        } catch (ContainerExecException cee) {
            result = cee.getResult();
            Assert.assertTrue(result.getStderr().contains("Namespace does not exist"), result.getStderr());
        }
    }

    @Test
    public void testNamespaceDeletionWithTopic() throws Exception {
        final String namespace = tenant + "/ns-with-topics";

        pulsarCluster.runAdminCommandOnAnyBroker("namespaces", "create", "-c",
                pulsarCluster.getClusterName(), namespace);

        for (int i = 0; i < 3; i++) {
            final String topic = "persistent://" + namespace + "/topic" + i;
            pulsarCluster.runAdminCommandOnAnyBroker("topics", "create", topic);
        }

        testDeleteNamespace(namespace);

        // recreate the namespace, and get topics, they should return 404 not found
        pulsarCluster.runAdminCommandOnAnyBroker("namespaces", "create", "-c",
                pulsarCluster.getClusterName(), namespace);
        for (int i = 0; i < 3; i++) {
            final String topic = "persistent://" + namespace + "/topic" + i;
            try {
                pulsarCluster.runAdminCommandOnAnyBroker("topics", "stats", topic);
            } catch (ContainerExecException cee) {
                ContainerExecResult result = cee.getResult();
                assertTrue(result.getStderr().contains("not found"));
            }
        }

        pulsarCluster.runAdminCommandOnAnyBroker("namespaces", "delete", "-f", namespace);
    }

    @Test
    public void testNamespaceDeletionWithFunctions() throws Exception {
        final String namespace = tenant + "/ns-with-functions";
        pulsarCluster.runAdminCommandOnAnyBroker("namespaces", "create", "-c",
                pulsarCluster.getClusterName(), namespace);

        for (int i = 0; i < 3; i++) {
            final String function = namespace + "/func-" + i;
            pulsarCluster.runAdminCommandOnAnyBroker("functions", "create", "--fqfn", function,
                    "--className", "org.apache.pulsar.functions.api.examples.ExclamationFunction", "--inputs",
                    "persistent://public/default/test-java-input", "--jar", JAR);
        }

        testDeleteNamespace(namespace);

        // recreate the namespace, and get functions, they should return 404 not found
        pulsarCluster.runAdminCommandOnAnyBroker("namespaces", "create", "-c",
                pulsarCluster.getClusterName(), namespace);
        for (int i = 0; i < 3; i++) {
            final String function = namespace + "/func-" + i;
            try {
                pulsarCluster.runAdminCommandOnAnyBroker("functions", "get", "--fqfn", function);
            } catch (ContainerExecException cee) {
                ContainerExecResult result = cee.getResult();
                assertTrue(result.getStderr().contains("doesn't exist"));
            }

        }

        pulsarCluster.runAdminCommandOnAnyBroker("namespaces", "delete", "-f", namespace);
    }

    @Test
    public void testNamespaceDeletionWithSinks() throws Exception {
        final String namespace = tenant + "/ns-with-sinks";
        pulsarCluster.runAdminCommandOnAnyBroker("namespaces", "create", "-c",
                pulsarCluster.getClusterName(), namespace);

        for (int i = 0; i < 3; i++) {
            final String sink = "sink-" + i;
            pulsarCluster.runAdminCommandOnAnyBroker("sinks", "create", "--tenant", tenant, "--namespace",
                    "ns-with-sinks", "--name", sink, "--className",
                    "org.apache.pulsar.tests.integration.io.TestGenericObjectSink", "--inputs",
                    "persistent://public/default/test-java-input", "--archive", JAR);
        }

        testDeleteNamespace(namespace);

        // recreate the namespace, and get sinks, they should return 404 not found
        pulsarCluster.runAdminCommandOnAnyBroker("namespaces", "create", "-c",
                pulsarCluster.getClusterName(), namespace);
        for (int i = 0; i < 3; i++) {
            final String sink = "sink-" + i;
            try {
                pulsarCluster.runAdminCommandOnAnyBroker("sinks", "get", "--tenant", tenant, "--namespace",
                        "ns-with-all", "--name", sink);
            } catch (ContainerExecException cee) {
                ContainerExecResult result = cee.getResult();
                assertTrue(result.getStderr().contains("doesn't exist"));
            }
        }

        pulsarCluster.runAdminCommandOnAnyBroker("namespaces", "delete", "-f", namespace);
    }

    @Test
    public void testNamespaceDeletionWithSources() throws Exception {
        final String namespace = tenant + "/ns-with-sources";
        pulsarCluster.runAdminCommandOnAnyBroker("namespaces", "create", "-c",
                pulsarCluster.getClusterName(), namespace);
        for (int i = 0; i < 3; i++) {
            final String source = "source-" + i;
            pulsarCluster.runAdminCommandOnAnyBroker("sources", "create", "--tenant", tenant,
                    "--namespace", "ns-with-sources", "--name", source, "--className",
                    "org.apache.pulsar.tests.integration.io.TestByteStateSource", "--destinationTopicName",
                    "persistent://public/default/test-java-output", "--archive", JAR);
        }

        testDeleteNamespace(namespace);

        // recreate the namespace, and get sources, they should return 404 not found
        pulsarCluster.runAdminCommandOnAnyBroker("namespaces", "create", "-c",
                pulsarCluster.getClusterName(), namespace);
        for (int i = 0; i < 3; i++) {
            final String source = "source-" + i;
            try {
                pulsarCluster.runAdminCommandOnAnyBroker("sources", "get",
                        "--tenant", tenant, "--namespace", "ns-with-all", "--name", source);
            } catch (ContainerExecException cee) {
                ContainerExecResult result = cee.getResult();
                assertTrue(result.getStderr().contains("doesn't exist"));
            }
        }

        pulsarCluster.runAdminCommandOnAnyBroker("namespaces", "delete", "-f", namespace);
    }

    @Test
    public void testNamespaceDeletionWithAllResources() throws Exception {
        final String namespace = tenant + "/ns-with-all";
        pulsarCluster.runAdminCommandOnAnyBroker("namespaces", "create", "-c",
                pulsarCluster.getClusterName(), namespace);

        for (int i = 0; i < 3; i++) {
            final String topic = "persistent://" + namespace + "/topic" + i;
            pulsarCluster.runAdminCommandOnAnyBroker("topics", "create", topic);
        }

        for (int i = 0; i < 3; i++) {
            final String function = namespace + "/func-" + i;
            pulsarCluster.runAdminCommandOnAnyBroker("functions", "create", "--fqfn", function,
                    "--className", "org.apache.pulsar.functions.api.examples.ExclamationFunction", "--inputs",
                    "persistent://public/default/test-java-input", "--jar", JAR);
        }

        for (int i = 0; i < 3; i++) {
            final String sink = "sink-" + i;
            pulsarCluster.runAdminCommandOnAnyBroker("sinks", "create", "--tenant", tenant, "--namespace",
                    "ns-with-all", "--name", sink, "--className",
                    "org.apache.pulsar.tests.integration.io.TestGenericObjectSink", "--inputs",
                    "persistent://public/default/test-java-input", "--archive", JAR);
        }

        for (int i = 0; i < 3; i++) {
            final String source = "source-" + i;
            pulsarCluster.runAdminCommandOnAnyBroker("sources", "create", "--tenant", tenant,
                    "--namespace", "ns-with-all", "--name", source, "--className",
                    "org.apache.pulsar.tests.integration.io.TestByteStateSource", "--destinationTopicName",
                    "persistent://public/default/test-java-output", "--archive", JAR);
        }

        testDeleteNamespace(namespace);

        // recreate the namespace, and get resources, they should return 404 not found
        pulsarCluster.runAdminCommandOnAnyBroker("namespaces", "create", "-c",
                pulsarCluster.getClusterName(), namespace);

        for (int i = 0; i < 3; i++) {
            final String topic = "persistent://" + namespace + "/topic" + i;
            try {
                pulsarCluster.runAdminCommandOnAnyBroker("topics", "stats", topic);
            } catch (ContainerExecException cee) {
                ContainerExecResult result = cee.getResult();
                assertTrue(result.getStderr().contains("not found"));
            }
        }

        for (int i = 0; i < 3; i++) {
            final String function = namespace + "/func-" + i;
            try {
                pulsarCluster.runAdminCommandOnAnyBroker("functions", "get", "--fqfn", function);
            } catch (ContainerExecException cee) {
                ContainerExecResult result = cee.getResult();
                assertTrue(result.getStderr().contains("doesn't exist"));
            }
        }

        for (int i = 0; i < 3; i++) {
            final String sink = "sink-" + i;
            try {
                pulsarCluster.runAdminCommandOnAnyBroker("sinks", "get", "--tenant", tenant, "--namespace",
                        "ns-with-all", "--name", sink);
            } catch (ContainerExecException cee) {
                ContainerExecResult result = cee.getResult();
                assertTrue(result.getStderr().contains("doesn't exist"));
            }
        }

        for (int i = 0; i < 3; i++) {
            final String source = "source-" + i;
            try {
                pulsarCluster.runAdminCommandOnAnyBroker("sources", "get",
                        "--tenant", tenant, "--namespace", "ns-with-all", "--name", source);
            } catch (ContainerExecException cee) {
                ContainerExecResult result = cee.getResult();
                assertTrue(result.getStderr().contains("doesn't exist"));
            }
        }

        pulsarCluster.runAdminCommandOnAnyBroker("namespaces", "delete", "-f", namespace);
    }

    private void testDeleteNamespace(String namespace) throws Exception {
        log.info("Deleting namespace without force parameter");
        ContainerExecResult result;
        try {
            pulsarCluster.runAdminCommandOnAnyBroker("namespaces", "delete", namespace);
        } catch (ContainerExecException cee) {
            result = cee.getResult();
            Assert.assertTrue(result.getStderr().contains("Cannot delete non empty namespace"), result.getStderr());
        }

        // the namespace should not be deleted
        result = pulsarCluster.runAdminCommandOnAnyBroker("namespaces", "get-clusters", namespace);
        assertEquals(0, result.getExitCode());
        assertTrue(result.getStdout().contains(pulsarCluster.getClusterName()));

        log.info("Deleting namespace with force parameter");
        result = pulsarCluster.runAdminCommandOnAnyBroker("namespaces", "delete", "-f", namespace);
        assertEquals(0, result.getExitCode());

        // the namespace should be deleted
        try {
            pulsarCluster.runAdminCommandOnAnyBroker("namespaces", "get-clusters", namespace);
        } catch (ContainerExecException cee) {
            result = cee.getResult();
            Assert.assertTrue(result.getStderr().contains("Namespace does not exist"), result.getStderr());
        }
    }
}
