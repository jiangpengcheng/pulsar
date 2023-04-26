const process = require('process');

function ClearTextSecretsProvider() {
    this.provideSecret = function (secretName, pathToSecret) {
        return pathToSecret;
    };
}

function EnvironmentBasedSecretsProvider() {
    this.provideSecret = function (secretName, pathToSecret) {
        return process.env[secretName];
    };
}

module.exports = {
    ClearTextSecretsProvider: ClearTextSecretsProvider,
    EnvironmentBasedSecretsProvider: EnvironmentBasedSecretsProvider
}