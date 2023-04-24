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

    function requireModule(path) {
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

    function getSchema(schemaType, typeClassname, schemaProperties) {
        if (typeClassname !== undefined && typeClassname.toLowerCase() === 'string') {
            return new StringSchema()
        }
        if (schemaType === undefined || schemaType === '') {
            return null
        } else if (schemaType.toLowerCase() === 'bytes') {
            return new ByteSchema()
        } else if (schemaType.toLowerCase() === 'string') {
            return new StringSchema()
        } else if (schemaType.toLowerCase() === 'json') {
            return new JsonSchema()
        } else if (schemaType.toLowerCase() === 'avro') {
            const definitions = requireModule(typeClassname)
            return new AvroSchema(definitions)
        } else {
            let schemaClass = requireModule(schemaType)
            return new schemaClass(schemaProperties)
        }
    }

    function setupGrpcClient() {
        let PROTO_PATH = __dirname + '/lib/Context.proto';
        let packageDefinition = protoLoader.loadSync(
            PROTO_PATH,
            {keepCase: true,
                longs: String,
                enums: String,
                defaults: true,
                oneofs: true
            });
        let protoDescriptor = grpc.loadPackageDefinition(packageDefinition).proto;
        return new protoDescriptor.Context('unix://' + __dirname + '/context.sock', grpc.credentials.createInsecure());
    }

    function setupLog(client, options) {
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
        let client = setupGrpcClient()
        // setup log
        setupLog(client, options)
        const logger = log4js.getLogger();
        const debugLogger = log4js.getLogger("debug");

        const rl = readline.createInterface({
            input: process.stdin,
        });
        let secretsProviderFunc = require('./lib/secrets_provider.js').ClearTextSecretsProvider
        if ('secrets_provider' in options && options['secretsProvider'] !== '') {
            if (options['secretsProvider'] === 'EnvironmentBasedSecretsProvider') {
                secretsProviderFunc = require('./lib/secrets_provider.js').EnvironmentBasedSecretsProvider
            } else {
                secretsProviderFunc = requireModule(options['secretsProvider'])
            }

        }
        let secretsProviderConfig = {}
        if ('secrets_provider_config' in options && options['secrets_provider_config'] !== '') {
            secretsProviderConfig = JSON.parse(options['secrets_provider_config'])
        }
        let secretsProvider = new secretsProviderFunc(secretsProviderConfig)

        let source = {}
        if ('source' in options && options['source'] !== '') {
            source = JSON.parse(options['source'])
        }
        let topicsToSerdeClassname = {}
        if ('topicsToSerDeClassName' in source) {
            topicsToSerdeClassname = source['topicsToSerDeClassName']
        }
        let inputSpec = {}
        if ('inputSpecs' in source) {
            inputSpec = source['inputSpecs']
        }
        let inputSerdes = {}
        let inputSchemas = {}
        for (const [topic, value] of Object.entries(topicsToSerdeClassname)) {
            let serdeClassname = require("./lib/serde")
            try {
                serdeClassname = requireModule(value)
            } catch (e) {
            }
            inputSerdes[topic] = new serdeClassname()
        }
        for (const [topic, value] of Object.entries(inputSpec)) {
            if ('serdeClassName' in value) {
                let serdeClassname = require("./lib/serde")
                try {
                    serdeClassname = requireModule(value['serdeClassName'])
                } catch (e) {
                }
                inputSerdes[topic] = new serdeClassname()
            }
            inputSchemas[topic] = getSchema(value['schemaType'], source['typeClassName'], source['schemaProperties'])
        }
        debugLogger.info("input_serdes: " + JSON.stringify(inputSerdes))
        debugLogger.info("input_schemas: " + JSON.stringify(inputSchemas))

        let sink = JSON.parse(options['sink'])
        let outputSchema = getSchema(sink["schemaType"], sink["typeClassName"], sink["schemaProperties"])
        let outputSerdeClass = require("./lib/serde.js")
        if ('serdeClassName' in sink && sink['serdeClassName'] !== '') {
            outputSerdeClass = requireModule(sink['serdeClassName'])
        }
        let outputSerde = new outputSerdeClass()
        debugLogger.info("output_serde: " +  outputSerde.constructor.name)
        debugLogger.info("output_schema: " + outputSchema.constructor.name)

        const contextObj = new context(options, secretsProvider, Object.keys(inputSerdes), sink['topic'], logger, client)

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
                if (topic in inputSchemas && inputSchemas[topic] !== null) {
                    debugLogger.info("topic: " + topic + ", using schema: " + inputSchemas[topic].constructor.name)
                    payload = inputSchemas[topic].decode(payload)
                } else if (topic in inputSerdes) {
                    debugLogger.info("topic: " + topic + ", using serde: " + inputSerdes[topic].constructor.name)
                    payload = inputSerdes[topic].deserialize(payload)
                }

                contextObj.setMessageId(msgId)
                contextObj.clearMessage()
                result = main(payload, contextObj)
                if (result instanceof Promise) {
                    result = await result
                }
                if (outputSchema !== null) {
                    result = outputSchema.encode(result)
                } else if ( outputSerde !== null) {
                    result =  outputSerde.serialize(result)
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
