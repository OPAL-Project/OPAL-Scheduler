const { Constants, ErrorHelper } =  require('eae-utils');
const mongodb = require('mongodb').MongoClient;
const MongoHelper = require('../src/mongoHelper');
let NodesWatchdog = require('../src/nodesWatchdog.js');

let mongoURL = 'mongodb://mongodb:27017/opal';

let options = {
    keepAlive: 30000, connectTimeoutMS: 30000,
};

function NodesWatchdogTestServer() {
    this.setup = NodesWatchdogTestServer.prototype.setup.bind(this);
    this.shutdown = NodesWatchdogTestServer.prototype.shutdown.bind(this);
    this.insertNode = NodesWatchdogTestServer.prototype.insertNode.bind(this);
}

NodesWatchdogTestServer.prototype.setup = function() {
    let _this = this;
    global.opal_scheduler_config = {
        nodesExpiredStatusTime: 1,
    };
    return new Promise(function(resolve, reject) {
        // Setup node env to test during test
        process.env.TEST = 1;

        mongodb.connect(mongoURL, options, function (err, mongo) {
            if (err !== null) {
                console.log('Could not connect to mongo: ' + err);
                reject(ErrorHelper('Could not connect to mongo: ', err));
            }
            _this.db = mongo;
            console.log('Connected to Mongo');

            _this.db.collection(Constants.EAE_COLLECTION_STATUS).deleteMany({}).then(() => {
                console.log('Cleared status collection');

                _this.mongo_helper = new MongoHelper();
                _this.mongo_helper.setCollections(_this.db.collection(Constants.EAE_COLLECTION_STATUS),
                    _this.db.collection(Constants.EAE_COLLECTION_JOBS),
                    _this.db.collection(Constants.EAE_COLLECTION_JOBS_ARCHIVE),
                    _this.db.collection(Constants.EAE_COLLECTION_FAILED_JOBS_ARCHIVE));

                _this.nodesWatchdog = new NodesWatchdog(_this.mongo_helper, null);

                console.log('Before all has been resolved');
                resolve(true);
            });
        });
    });
};

NodesWatchdogTestServer.prototype.shutdown = function() {
    let _this = this;
    return new Promise(function (resolve, reject) {
        _this.db.close();
        console.log('Resolved afterEach()');
        resolve(true);
    });
};

NodesWatchdogTestServer.prototype.insertNode = function(node) {
    let _this = this;
    return new Promise(function(resolve, reject) {
        _this.db.collection(Constants.EAE_COLLECTION_STATUS).insertOne(node).then(function(document) {
            resolve(document);
        });
    });
};

module.exports = NodesWatchdogTestServer;
