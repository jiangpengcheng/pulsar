const { Buffer } = require('node:buffer');
const avro = require('avro-js');

function ByteSchema() {
    this.type = 'BYTES';
    this.encode = function (data) {
        return data;
    }
    this.decode = function (data) {
        return data;
    }
}

function StringSchema() {
    this.type = 'STRING';
    this.encode = function (data) {
        return Buffer.from(data, 'utf-8');
    }
    this.decode = function (data) {
        return data.toString().trim();
    }
}

function JsonSchema() {
    this.type = 'JSON';
    this.encode = function (data) {
        return Buffer.from(JSON.stringify(data), 'utf-8');
    }
    this.decode = function (data) {
        return JSON.parse(data.toString().trim());
    }
}

function AvroSchema(properties) {
    this.parser = avro.parse(properties)
    this.type = 'AVRO';
    this.encode = function (data) {
        return this.parser.toBuffer(data);
    }
    this.decode = function (data) {
        return this.parser.fromBuffer(data);
    }
}

module.exports = {
    ByteSchema, StringSchema, JsonSchema, AvroSchema
}