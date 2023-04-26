const IdentitySerDe = require("./serde");
const promisify = require('util').promisify

function context(options, secretsProvider, inputTopics, outputTopic, logger, client) {
    this.options = options
    this.inputTopics = inputTopics
    this.outputTopic = outputTopic
    this.client = client
    this.defaultSerde = new IdentitySerDe()
    const currentRecord = promisify(this.client.currentRecord).bind(this.client)
    const getState = promisify(this.client.getState).bind(this.client)
    const putState = promisify(this.client.putState).bind(this.client)
    const deleteState = promisify(this.client.deleteState).bind(this.client)
    const getCounter = promisify(this.client.getCounter).bind(this.client)
    const incrCounter = promisify(this.client.incrCounter).bind(this.client)
    const recordMetrics = promisify(this.client.recordMetrics).bind(this.client)
    const publish = promisify(this.client.publish).bind(this.client)

    this.userConfig = {}
    if ('user_config' in options && options['user_config'] !== '') {
        this.userConfig = JSON.parse(options['user_config'])
    }

    this.secretsMap = {}
    if ('secrets_map' in options && options['secrets_map'] !== '') {
        this.secretsMap = JSON.parse(options['secrets_map'])
    }
    this.secretsProvider = secretsProvider

    this.logger = logger
    this.message = null
    this.messageId = ""
    this.publishSerializers = {}

    this.getMessageId = async function () {
        if (this.message === null) {
            this.message = await currentRecord({
                id: this.messageId
            })
        }
        return this.message["messageId"]
    }

    this.getMessageKey = async function () {
        if (this.message === null) {
            this.message = await currentRecord({
                id: this.messageId
            })
        }
        return this.message["key"]
    }

    this.getMessageEventTime = async function () {
        if (this.message === null) {
            this.message = await currentRecord({
                id: this.messageId
            })
        }
        return this.message["eventTimestamp"]
    }

    this.getMessageProperties = async function () {
        if (this.message === null) {
            this.message = await currentRecord({
                id: this.messageId
            })
        }
        return JSON.parse(this.message["properties"])
    }

    this.getCurrentMessageTopicName = async function () {
        if (this.message === null) {
            this.message = await currentRecord({
                id: this.messageId
            })
        }
        return this.message["topicName"]
    }

    this.getMessagePartitionId = async function () {
        if (this.message === null) {
            this.message = await currentRecord({
                id: this.messageId
            })
        }
        return this.message["partitionId"]
    }

    this.getFunctionName = function () {
        return this.options['name']
    }

    this.getFunctionTenant = function () {
        return this.options['tenant']
    }

    this.getFunctionNamespace = function () {
        return this.options['namespace']
    }

    this.getFunctionId = function () {
        return this.options['function_id']
    }

    this.getInstanceId = function () {
        return this.options['instance_id']
    }

    this.getFunctionVersion = function () {
        return this.options['function_version']
    }

    this.getLogger = function () {
        return this.logger
    }

    this.getUserConfigValue = function (key) {
        if (key in this.userConfig) {
            return this.userConfig[key]
        }
        return null
    }

    this.getUserConfigMap = function () {
        return this.userConfig
    }

    this.getSecret = function (secretKey) {
        if (!(secretKey in this.secretsMap)) {
            return null
        }
        return this.secretsProvider.provideSecret(secretKey, this.secretsMap[secretKey])
    }

    this.getInputTopics = function () {
        return this.inputTopics
    }

    this.getOutputTopic = function () {
        return this.outputTopic
    }

    this.clearMessage = function (message) {
        this.message = null
    }

    this.setMessageId = function (messageId) {
        this.messageId = messageId
    }

    this.getStateKey = function (key) {
        return getState({
            key: key
        })
    }

    this.putStateKey = function (key, value) {
        return putState({
            key: key,
            value: Buffer.from(value),
        })
    }

    this.deleteStateKey = function (key) {
        return deleteState({
            key: key
        })
    }

    this.getCounter = function (key) {
        return getCounter({
            key: key
        })
    }

    this.incrementCounter = function (key, amount) {
        return incrCounter({
            key: key,
            amount: amount
        })
    }

    this.recordMetrics = function (metricName, metricValue) {
        return recordMetrics({
            metricName: metricName,
            value: metricValue
        })
    }

    this.publish = function (topic, message, serdeClassName, messageConf) {
        let bytes
        if (!! serdeClassName) {
            if (!( serdeClassName in this.publishSerializers)) {
                let serdeClass = require( serdeClassName)
                this.publishSerializers[ serdeClassName] = new serdeClass()
            }
            bytes = this.publishSerializers[ serdeClassName].serialize(message)
        } else {
            bytes = this.defaultSerde.serialize(message)
        }
        let msg = {
            topic: topic,
            payload: bytes,
            ...messageConf
        }

        return publish(msg)
    }
}

module.exports = context