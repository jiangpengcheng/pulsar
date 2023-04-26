const { Buffer } = require('node:buffer')

function IdentitySerDe() {
    this.serialize = function (input) {
        if (typeof input === 'string') {
            return Buffer.from(input, 'utf-8')
        } else if (input instanceof Buffer) {
            return input
        } else if (typeof input === 'number') {
            return Buffer.from(input.toString(), 'utf-8')
        }
        throw new Error('Unsupported type for serialization: ' + typeof input)
    };
    this.deserialize = function (inputBytes) {
        try {
            return inputBytes.toString('utf-8')
        } catch (e) {
        }
        return inputBytes
    };
}

module.exports = IdentitySerDe