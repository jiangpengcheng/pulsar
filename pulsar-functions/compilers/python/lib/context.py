#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

# -*- encoding: utf-8 -*-

"""contextimpl.py: ContextImpl class that implements the Context interface
"""

import pulsar

from . import Context_pb2
from . import utils


class ContextImpl(pulsar.Context):
    def ack(self, msgid, topic):
        pass

    def __init__(self, tenant, namespace, name, function_id, instance_id, function_version, logger, input_topics,
                 output_topic, output_serde, user_code_dir, user_config, secrets_map, secrets_provider, state_context,
                 stub):
        self.tenant = tenant
        self.namespace = namespace
        self.name = name
        self.function_id = function_id
        self.instance_id = instance_id
        self.function_version = function_version
        self.log = logger
        self.secrets_provider = secrets_provider
        self.state_context = state_context
        self.publish_producers = {}
        self.publish_serializers = {}
        self.message = {}
        self.input_topics = input_topics
        self.output_topic = output_topic
        self.output_serde = output_serde
        self.user_code_dir = user_code_dir
        self.user_config = user_config
        self.secrets_map = secrets_map
        self.stub = stub

    def get_message_id(self):
        return self.message.get('messageId', None)

    def get_message_key(self):
        return self.message.get('key', None)

    def get_message_eventtime(self):
        return self.message.get('eventTime', None)

    def get_message_properties(self):
        return self.message.get('properties', {})

    def get_current_message_topic_name(self):
        return self.message.get('topicName', None)

    def get_partition_key(self):
        return self.message.get('partitionId', None)

    def get_function_name(self):
        return self.name

    def get_function_tenant(self):
        return self.tenant

    def get_function_namespace(self):
        return self.namespace

    def get_function_id(self):
        return self.function_id

    def get_instance_id(self):
        return self.instance_id

    def get_function_version(self):
        return self.function_version

    def get_logger(self):
        return self.log

    def get_user_config_value(self, key):
        if key in self.user_config:
            return self.user_config[key]
        else:
            return None

    def get_user_config_map(self):
        return self.user_config

    def get_secret(self, secret_key):
        if not secret_key in self.secrets_map:
            return None
        return self.secrets_provider.provide_secret(secret_key, self.secrets_map[secret_key])

    def record_metric(self, metric_name, metric_value):
        if metric_name not in self.user_metrics_map:
            user_metrics_labels = self.metrics_labels + [metric_name]
            self.user_metrics_map[metric_name] = self.user_metrics_summary.labels(*user_metrics_labels)

        self.user_metrics_map[metric_name].observe(metric_value)

    def get_input_topics(self):
        return self.input_topics

    def get_output_topic(self):
        return self.output_topic

    def get_output_serde_class_name(self):
        return self.output_serde

    def publish(self, topic_name, message, serde_class_name="serde.IdentitySerDe", properties=None,
                compression_type=None, callback=None, message_conf=None):
        # Just make sure that user supplied values are properly typed
        topic_name = str(topic_name)
        serde_class_name = str(serde_class_name)

        if serde_class_name not in self.publish_serializers:
            serde_klass = utils.import_class(self.user_code_dir, serde_class_name)
            self.publish_serializers[serde_class_name] = serde_klass()

        output_bytes = bytes(self.publish_serializers[serde_class_name].serialize(message))

        if properties:
            # The deprecated properties args was passed. Need to merge into message_conf
            if not message_conf:
                message_conf = {}
            message_conf['properties'] = properties

        if message_conf:
            self.stub.Publish(Context_pb2.PulsarMessage(topic=topic_name, payload=output_bytes, **message_conf))
        else:
            self.stub.Publish(Context_pb2.PulsarMessage(topic=topic_name, payload=output_bytes))

    def incr_counter(self, key, amount):
        return self.state_context.incr(key, amount)

    def get_counter(self, key):
        return self.state_context.get_amount(key)

    def del_counter(self, key):
        return self.state_context.delete(key)

    def put_state(self, key, value):
        return self.state_context.put(key, value)

    def get_state(self, key):
        return self.state_context.get_value(key)

    def get_pulsar_client(self):
        return None

    def set_current_msg(self, message):
        self.message = message
