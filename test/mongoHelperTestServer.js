const { Constants, ErrorHelper } =  require('eae-utils');
const mongodb = require('mongodb').MongoClient;
const MongoHelper = require('../src/mongoHelper');

let mongoURL = 'mongodb://mongodb:27017/opal';

let options = {
    keepAlive: 30000, connectTimeoutMS: 30000,
};

function MongoHelperTestServer() {
    this.setup = MongoHelperTestServer.prototype.setup.bind(this);
    this.shutdown = MongoHelperTestServer.prototype.shutdown.bind(this);
}

MongoHelperTestServer.prototype.setup = function() {
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

            _this.db.collection(Constants.EAE_COLLECTION_JOBS).deleteMany({}).then(() => {
                console.log('Cleared jobs collection');
                _this.db.collection(Constants.EAE_COLLECTION_STATUS).deleteMany({}).then(() => {
                    console.log('Cleared status collection');
                    _this.db.collection(Constants.EAE_COLLECTION_JOBS).deleteMany({}).then(() => {
                        _this.db.collection(Constants.EAE_COLLECTION_JOBS_ARCHIVE).deleteMany({}).then(() => {
                            _this.db.collection(Constants.EAE_COLLECTION_FAILED_JOBS_ARCHIVE).deleteMany({}).then(() => {

                                _this.mongo_helper = new MongoHelper();
                                _this.mongo_helper.setCollections(_this.db.collection(Constants.EAE_COLLECTION_STATUS),
                                    _this.db.collection(Constants.EAE_COLLECTION_JOBS),
                                    _this.db.collection(Constants.EAE_COLLECTION_JOBS_ARCHIVE),
                                    _this.db.collection(Constants.EAE_COLLECTION_FAILED_JOBS_ARCHIVE));

                                console.log('Before all has been resolved');
                                resolve(true);
                            });
                        });
                    });
                });
            });
        });
    });
};

MongoHelperTestServer.prototype.shutdown = function() {
    let _this = this;
    return new Promise(function (resolve, reject) {
        _this.db.close();
        console.log('Resolved afterEach()');
        resolve(true);
    });
};

module.exports = MongoHelperTestServer;
