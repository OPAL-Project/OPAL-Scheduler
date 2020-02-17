const {Constants}  =  require('eae-utils');

module.exports = {
    mongoURL: 'mongodb://mongodb:27017',
    port: 80,
    enableCors: true,
    swiftURL: 'swift://swift:8080',
    swiftUsername: 'test',
    swiftPassword: 'test',
    computeType: [Constants.EAE_COMPUTE_TYPE_PYTHON2],
    clusters:{}
};
