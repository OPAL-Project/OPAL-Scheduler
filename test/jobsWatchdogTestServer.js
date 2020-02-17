const { Constants, ErrorHelper } =  require('eae-utils');
const mongodb = require('mongodb').MongoClient;
const MongoHelper = require('../src/mongoHelper');
let JobsWatchdog = require('../src/jobsWatchdog.js');

let mongoURL = 'mongodb://mongodb:27017/opal';
// let mongoURL = 'mongodb://localhost:27017';

let options = {
    keepAlive: 30000, connectTimeoutMS: 30000,
};

function JobsWatchdogTestServer() {
    this.setup = JobsWatchdogTestServer.prototype.setup.bind(this);
    this.shutdown = JobsWatchdogTestServer.prototype.shutdown.bind(this);
    this.insertNode = JobsWatchdogTestServer.prototype.insertNode.bind(this);
    this.insertJob = JobsWatchdogTestServer.prototype.insertJob.bind(this);
}

JobsWatchdogTestServer.prototype.setup = function() {
    let _this = this;
    global.opal_scheduler_config = {
        jobsExpiredStatusTime: 1,
        jobsTimingoutTime: 1
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

            _this.db.collection(Constants.EAE_COLLECTION_JOBS).deleteMany({}).then(() => {
                console.log('Cleared jobs collection');
                _this.db.collection(Constants.EAE_COLLECTION_JOBS_ARCHIVE).deleteMany({}).then(() => {
                    console.log('Cleared archived jobs collection');
                    _this.db.collection(Constants.EAE_COLLECTION_STATUS).deleteMany({}).then(() => {
                        console.log('Cleared status collection');
                        let node = {
                            ip: 'compute',
                            port: 80,
                            status: Constants.EAE_SERVICE_STATUS_IDLE,
                            computeType: 'r',
                            statusLock: false
                        };

                        _this.db.collection(Constants.EAE_COLLECTION_STATUS).insertOne(node).then(() => {
                            console.log('Added idle worker');
                            _this.mongo_helper = new MongoHelper();
                            _this.mongo_helper.setCollections(_this.db.collection(Constants.EAE_COLLECTION_STATUS),
                                _this.db.collection(Constants.EAE_COLLECTION_JOBS),
                                _this.db.collection(Constants.EAE_COLLECTION_JOBS_ARCHIVE),
                                _this.db.collection(Constants.EAE_COLLECTION_FAILED_JOBS_ARCHIVE));

                            _this.jobsWatchdog = new JobsWatchdog(_this.mongo_helper, null);

                            console.log('Before all has been resolved');
                            resolve(true);
                        });
                    });
                });
            });
        });
    });
};

JobsWatchdogTestServer.prototype.shutdown = function() {
    let _this = this;
    return new Promise(function (resolve, reject) {
        _this.db.close();
        console.log('Resolved afterEach()');
        resolve(true);
    });
};

JobsWatchdogTestServer.prototype.insertJob = function(job) {
    let _this = this;
    return new Promise(function(resolve, reject) {
        _this.db.collection(Constants.EAE_COLLECTION_JOBS).insertOne(job).then(function(document) {
            resolve(document);
        });
    });
};

JobsWatchdogTestServer.prototype.insertNode = function(node) {
    let _this = this;
    return new Promise(function(resolve, reject) {
        _this.db.collection(Constants.EAE_COLLECTION_STATUS).insertOne(node).then(function(document) {
            resolve(document);
        });
    });
};

module.exports = JobsWatchdogTestServer;
