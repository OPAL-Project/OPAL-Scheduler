const { Constants, ErrorHelper } =  require('eae-utils');
const mongodb = require('mongodb').MongoClient;
const MongoHelper = require('../src/mongoHelper');
let JobsScheduler = require('../src/jobsScheduler.js');

let mongoURL = 'mongodb://mongodb:27017/opal';

let options = {
    keepAlive: 30000, connectTimeoutMS: 30000,
};

function JobsSchedulerTestServer() {
    this.setup = JobsSchedulerTestServer.prototype.setup.bind(this);
    this.shutdown = JobsSchedulerTestServer.prototype.shutdown.bind(this);
    this.insertNode = JobsSchedulerTestServer.prototype.insertNode.bind(this);
    this.insertJob = JobsSchedulerTestServer.prototype.insertJob.bind(this);
    this.dbCleanup = JobsSchedulerTestServer.prototype.dbCleanup.bind(this);
}

JobsSchedulerTestServer.prototype.setup = function() {
    let _this = this;
    return new Promise(function(resolve, reject) {
        // Setup node env to test during test
        process.env.TEST = 1;
        mongodb.connect(mongoURL, options, function (err, mongo) {
            if (err !== null) {
                reject(ErrorHelper('Could not connect to mongo: ', err));
            } else {
                _this.db = mongo;
                console.log('Connected to Mongo');
                resolve(true);
            }
        });
    });
};

JobsSchedulerTestServer.prototype.dbCleanup = function() {
    let _this = this;
    return new Promise(function (resolve, reject){
        _this.db.collection(Constants.EAE_COLLECTION_JOBS).deleteMany({}).then(() => {
            console.log('Cleared jobs collection');
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
    
                    _this.jobsScheduler = new JobsScheduler(_this.mongo_helper);
                    console.log('Before all has been resolved');
                    resolve(true);
                });
            });
        });
    });
}

JobsSchedulerTestServer.prototype.shutdown = function() {
    let _this = this;
    return new Promise(function (resolve, reject) {
        _this.db.close();
        console.log('Resolved afterEach()');
        resolve(true);
    });
};

JobsSchedulerTestServer.prototype.insertJob = function(job) {
    let _this = this;
    return new Promise(function(resolve, reject) {
        _this.db.collection(Constants.EAE_COLLECTION_JOBS).insertOne(job).then(function(document) {
            resolve(document);
        });
    });
};

JobsSchedulerTestServer.prototype.insertNode = function(node) {
    let _this = this;
    return new Promise(function(resolve, reject) {
        _this.db.collection(Constants.EAE_COLLECTION_STATUS).insertOne(node).then(function(document) {
          resolve(document);
        });
    });
};

module.exports = JobsSchedulerTestServer;
