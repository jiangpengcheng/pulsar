#!/usr/bin/env python3
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

import argparse
import logging
import os
import pathlib

import grpc
import json
import traceback
from sys import stderr
from sys import stdin
from sys import stdout

from lib import context, Context_pb2_grpc, log, state_context, utils

Log = log.Log
PULSAR_API_ROOT = 'pulsar'
PULSAR_FUNCTIONS_API_ROOT = 'functions'


def main():
    parser = argparse.ArgumentParser(description='Pulsar Functions Python Instance')
    parser.add_argument('--tenant', required=True, help='tenant of function')
    parser.add_argument('--namespace', required=True, help='namespace of function')
    parser.add_argument('--name', required=True, help='name of function')
    parser.add_argument('--source', required=True, help='the source spec')
    parser.add_argument('--sink', required=True, help='the sink spec')
    parser.add_argument('--instance_id', required=True, help='Instance Id')
    parser.add_argument('--function_id', required=True, help='Function Id')
    parser.add_argument('--function_version', required=True, help='Function Version')
    parser.add_argument('--user_config', required=False, help='User config')
    parser.add_argument('--secrets_map', required=False, help='Secrets map')
    parser.add_argument('--secrets_provider', required=False, help='The classname of the secrets provider')
    parser.add_argument('--secrets_provider_config', required=False, help='The config that needs to be passed to '
                                                                          'secrets provider')
    parser.add_argument('--state_storage_serviceurl', required=False, help='Managed State Storage Service Url')
    parser.add_argument('--log_topic', required=False, help='The log topic')
    args = parser.parse_args()

    work_dir = str(pathlib.Path(__file__).parent.resolve())

    state_storage_serviceurl = None
    if args.state_storage_serviceurl is not None:
        state_storage_serviceurl = str(args.state_storage_serviceurl)

    if args.secrets_provider is not None:
        secrets_provider = utils.import_class(work_dir, str(args.secrets_provider))
    else:
        from lib import secrets_provider
        secrets_provider = secrets_provider.ClearTextSecretsProvider
    secrets_provider = secrets_provider()
    secrets_provider_config = None
    if args.secrets_provider_config is not None:
        args.secrets_provider_config = str(args.secrets_provider_config)
        if args.secrets_provider_config[0] == '\'':
            args.secrets_provider_config = args.secrets_provider_config[1:]
        if args.secrets_provider_config[-1] == '\'':
            args.secrets_provider_config = args.secrets_provider_config[:-1]
        secrets_provider_config = json.loads(str(args.secrets_provider_config))
    secrets_provider.init(secrets_provider_config)

    function_class, function_pure_function = utils.import_function_class(work_dir, "CLASS_NAME")

    source = json.loads(args.source) if args.source else {}
    topics_to_serde_class_name = source["topicsToSerDeClassName"] if "topicsToSerDeClassName" in source else {}
    input_spec = source["inputSpecs"] if "inputSpecs" in source else {}
    input_serdes, input_schemas = utils.setup_input_serde(topics_to_serde_class_name, input_spec,
                                                          source["typeClassName"],
                                                          work_dir)
    sink = json.loads(args.sink) if args.sink else {}
    output_schema_properties = sink["schemaProperties"] if "schemaProperties" in sink else {}
    output_serde, output_schema = utils.setup_output_serde(sink.get("serdeClassName"), sink.get("schemaType"),
                                                           sink.get("typeClassName"), output_schema_properties,
                                                           work_dir)

    channel = grpc.insecure_channel('unix://' + work_dir + "/context.sock")
    stub = Context_pb2_grpc.ContextStub(channel)

    if args.log_topic:
        log_file = os.path.join(work_dir,
                                utils.get_fully_qualified_function_name(args.tenant, args.namespace, args.name),
                                "log-%s.log" % args.instance_id)
        log_grpc_handler = log.LogGRPCHandler(args.log_topic, stub)
        log.remove_all_handlers()
        log.add_handler(log_grpc_handler)
        log.init_logger(logging.INFO, log_file)

    user_config = json.loads(args.user_config) if args.user_config else {}
    secrets_map = json.loads(args.secrets_map) if args.secrets_map else {}

    #table_ns = "%s_%s" % (args.tenant, args.namespace)
    #table_ns = table_ns.replace("-", "_")
    state = state_context.NullStateContext()
    #state = state_context.create_state_context(state_storage_serviceurl, table_ns, str(args.name))

    context_impl = context.ContextImpl(args.tenant, args.namespace, args.name, args.function_id, args.instance_id,
                                       args.function_version, log.Log, list(input_serdes.keys()), sink['topic'],
                                       type(output_serde).__name__, work_dir, user_config, secrets_map,
                                       secrets_provider, state, stub)

    while True:
        topic = stdin.buffer.readline().decode('utf-8').rstrip()
        if not topic:
            continue

        msg = stdin.buffer.readline().rstrip()
        if not msg:
            continue

        try:
            # deserialize the input
            if input_schemas.get(topic) is not None:
                msg = input_schemas[topic].decode(msg)
            elif input_serdes.get(topic) is not None:
                msg = input_serdes[topic].deserialize(msg)

            if function_class is not None:
                res = function_class.process(msg, context_impl)
            else:
                res = function_pure_function.process(msg)

            # serialize output to bytes
            if output_schema is not None:
                res = output_schema.encode(res)
            elif output_serde is not None:
                res = output_serde.serialize(res)
        except Exception as ex:
            print(traceback.format_exc(), file=stderr)
            res = ("error: %s" % str(ex)).encode('utf-8')

        stdout.buffer.write(res)
        stdout.buffer.write('\n'.encode('utf-8'))
        stdout.flush()
        stderr.flush()


if __name__ == '__main__':
    main()
