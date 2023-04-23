#!/usr/bin/env node

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

try {
    const main = require("main__").process
    const {AvroSchema, ByteSchema, JsonSchema, StringSchema} = require("./lib/schemas")
    const { Buffer } = require('node:buffer')
    const readline = require('readline');
    const log4js = require("log4js");
    const context = require("./lib/context.js")
    const commandLineArgs = require('command-line-args')
    const grpc = require('@grpc/grpc-js');
    const protoLoader = require('@grpc/proto-loader');

    const optionDefinitions = [
        { name: 'tenant', type: String},
        { name: 'namespace', type: String},
        { name: 'name', type: String},
        { name: 'source', type: String},
        { name: 'sink', type: String},
        { name: 'instance_id', type: String},
        { name: 'function_id', type: String},
        { name: 'function_version', type: String},
        { name: 'user_config', type: String},
        { name: 'secrets_map', type: String},
        { name: 'secrets_provider', type: String},
        { name: 'secrets_provider_config', type: String},
        { name: 'state_storage_serviceurl', type: String},
        { name: 'log_topic', type: String},
    ]

    function require_module(path) {
        let idx = path.lastIndexOf('.')
        try {
            let filePath = path.slice(0, idx)
            if (!filePath.startsWith('./')) {
                filePath = './' + filePath
            }
            let module = require(filePath)
            return module[path.slice(idx + 1)]
        } catch (e) {
            return require(path)
        }
    }

    function get_schema(schema_type, type_class_name, schema_properties) {
        if (type_class_name !== undefined && type_class_name.toLowerCase() === 'string') {
            return new StringSchema()
        }
        if (schema_type === undefined || schema_type === '' || schema_type.toLowerCase() === 'bytes') {
            return new ByteSchema()
        } else if (schema_type.toLowerCase() === 'string') {
            return new StringSchema()
        } else if (schema_type.toLowerCase() === 'json') {
            return new JsonSchema()
        } else if (schema_type.toLowerCase() === 'avro') {
            const definitions = require_module(type_class_name)
            return new AvroSchema(definitions)
        } else {
            let schema_class = require_module(schema_type)
            return new schema_class(schema_properties)
        }
    }

    function setup_grpc_client() {
        let PROTO_PATH = __dirname + '/lib/Context.proto';
        let packageDefinition = protoLoader.loadSync(
            PROTO_PATH,
            {keepCase: true,
                longs: String,
                enums: String,
                defaults: true,
                oneofs: true
            });
        let protoDescriptor = grpc.loadPackageDefinition(packageDefinition).contextproto;
        return new protoDescriptor.Context('unix://' + __dirname + '/context.sock', grpc.credentials.createInsecure());
    }

    function setup_log(client, options) {
        const GrpcAppenderModule = {
            configure: function(config, layouts) {
                let layout = layouts.basicLayout;
                return (loggingEvent) => {
                    let message = {
                        topic: options['log_topic'],
                        payload: Buffer.from(layout(loggingEvent), 'latin1')
                    }
                    client.publish(message, function(err, response) {
                    });
                };
            }
        }
        let appenders = { debug: { type: 'file', filename: "debug.log"}}
        let categories = { default: { appenders: ['debug'], level: 'info' } }
        if ('log_topic' in options && options['log_topic'] !== '') {
            appenders = { debug: { type: 'file', filename: "debug.log"}, grpc: {type: GrpcAppenderModule}}
            categories = { default: { appenders: ['grpc'], level: 'info' }, debug: { appenders: ['debug'], level: 'info'} }
        }
        log4js.configure({
            appenders: appenders,
            categories: categories
        })
    }
    process.stdin.setEncoding('latin1')
    process.stdout.setEncoding('latin1')

    async function actionLoop() {
        const options = commandLineArgs(optionDefinitions)

        // set grpc client
        let client = setup_grpc_client()
        // setup log
        setup_log(client, options)
        const logger = log4js.getLogger();
        const debugLogger = log4js.getLogger("debug");

        const rl = readline.createInterface({
            input: process.stdin,
        });
        let secrets_provider_func = require('./lib/secrets_provider.js').ClearTextSecretsProvider
        if ('secrets_provider' in options && options['secrets_provider'] !== '') {
            if (options['secrets_provider'] === 'EnvironmentBasedSecretsProvider') {
                secrets_provider_func = require('./lib/secrets_provider.js').EnvironmentBasedSecretsProvider
            } else {
                secrets_provider_func = require_module(options['secrets_provider'])
            }

        }
        let secrets_provider_config = {}
        if ('secrets_provider_config' in options && options['secrets_provider_config'] !== '') {
            secrets_provider_config = JSON.parse(options['secrets_provider_config'])
        }
        let secrets_provider = new secrets_provider_func(secrets_provider_config)

        let source = {}
        if ('source' in options && options['source'] !== '') {
            source = JSON.parse(options['source'])
        }
        let topics_to_serde_class_name = {}
        if ('topicsToSerDeClassName' in source) {
            topics_to_serde_class_name = source['topicsToSerDeClassName']
        }
        let input_spec = {}
        if ('inputSpecs' in source) {
            input_spec = source['inputSpecs']
        }
        let input_serdes = {}
        let input_schemas = {}
        for (const [topic, value] of Object.entries(topics_to_serde_class_name)) {
            let serde_class_name = require("./lib/serde")
            try {
                serde_class_name = require_module(value)
            } catch (e) {
            }
            input_serdes[topic] = new serde_class_name()
        }
        for (const [topic, value] of Object.entries(input_spec)) {
            if ('serdeClassName' in value) {
                let serde_class_name = require("./lib/serde")
                try {
                    serde_class_name = require_module(value['serdeClassName'])
                } catch (e) {
                }
                input_serdes[topic] = new serde_class_name()
            }
            input_schemas[topic] = get_schema(value['schemaType'], source['typeClassName'], source['schemaProperties'])
        }
        debugLogger.info("input_serdes: " + JSON.stringify(input_serdes))
        debugLogger.info("input_schemas: " + JSON.stringify(input_schemas))

        let sink = JSON.parse(options['sink'])
        let output_schema = get_schema(sink["schemaType"], sink["typeClassName"], sink["schemaProperties"])
        let output_serde_class = require("./lib/serde.js")
        if ('serdeClassName' in sink && sink['serdeClassName'] !== '') {
            output_serde_class = require_module(sink['serdeClassName'])
        }
        let output_serde = new output_serde_class()
        debugLogger.info("output_serde: " + output_serde.constructor.name)
        debugLogger.info("output_schema: " + output_schema.constructor.name)

        const contextObj = new context(options, secrets_provider, Object.keys(input_serdes), sink['topic'], logger, client)

        rl.on('line', async (line) => {
            let chunk = Buffer.from(line, 'latin1')
            let metaLength = chunk.readInt8()
            let meta = chunk.slice(1, metaLength + 1).toString().split('@')
            if (meta.length !== 2) {
                throw new Error("Invalid message format: " + meta.join(","))
            }
            let msgId = meta[0]
            let topic = meta[1]
            debugLogger.info("topic is: " + topic)

            let result = ''
            try {
                // deserialize payload
                let payload = chunk.slice(1 + metaLength)
                if (topic in input_schemas) {
                    debugLogger.info("topic: " + topic + ", using schema: " + input_schemas[topic].constructor.name)
                    payload = input_schemas[topic].decode(payload)
                } else if (topic in input_serdes) {
                    debugLogger.info("topic: " + topic + ", using serde: " + input_serdes[topic].constructor.name)
                    payload = input_serdes[topic].deserialize(payload)
                }

                contextObj.set_message_id(msgId)
                contextObj.clear_message()
                result = main(payload, contextObj)
                if (result instanceof Promise) {
                    result = await result
                }
                if (output_schema !== null) {
                    result = output_schema.encode(result)
                } else if (output_serde !== null) {
                    result = output_serde.serialize(result)
                }
            } catch (err) {
                let message = err.message || err.toString()
                result = Buffer.from("error:" + message, 'latin1')
            }
            process.stdout.write(result)
            process.stdout.write('\n')
        });
    }
    actionLoop()
} catch (e) {
    if (e.code == "MODULE_NOT_FOUND") {
        console.error("zipped actions must contain either package.json or index.js at the root.")
    }
    console.error(e)
    process.exit(1)
}
