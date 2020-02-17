//External node module imports
const mongodb = require('mongodb').MongoClient;
const express = require('express');
const body_parser = require('body-parser');
const { ErrorHelper, StatusHelper, SwiftHelper, Constants } = require('eae-utils');

const MongoHelper = require('./mongoHelper');

const package_json = require('../package.json');
const StatusController = require('./statusController.js');
const JobsScheduler = require('./jobsScheduler');
const JobsWatchdog = require('./jobsWatchdog');
const NodesWatchdog = require('./nodesWatchdog');

/**
 * @class OpalScheduler
 * @desc Core class of the scheduler microservice
 * @param config Configurations for the scheduler
 * @constructor
 */
function OpalScheduler(config) {
    // Init member attributes
    this.config = config;
    this.app = express();
    global.opal_scheduler_config = config;
    global.opal_compute_nodes_status = [];
    this.mongo_helper = new MongoHelper();
    this.swift_helper = null;

    // Bind public member functions
    this.start = OpalScheduler.prototype.start.bind(this);
    this.stop = OpalScheduler.prototype.stop.bind(this);

    // Bind private member functions
    this._connectDb = OpalScheduler.prototype._connectDb.bind(this);
    this._setupStatusController = OpalScheduler.prototype._setupStatusController.bind(this);
    this._setupMongoHelper = OpalScheduler.prototype._setupMongoHelper.bind(this);
    this._setupSwiftHelper = OpalScheduler.prototype._setupSwiftHelper.bind(this);
    this._setupNodesWatchdog = OpalScheduler.prototype._setupNodesWatchdog.bind(this);
    this._setupJobsScheduler = OpalScheduler.prototype._setupJobsScheduler.bind(this);

    //Remove unwanted express headers
    this.app.set('x-powered-by', false);

    //Allow CORS requests when enabled
    if (this.config.enableCors === true) {
        this.app.use(function (_unused__req, res, next) {
            res.header('Access-Control-Allow-Origin', '*');
            res.header('Access-Control-Allow-Headers', 'Origin, X-Requested-With, Content-Type, Accept');
            next();
        });
    }

    // Init third party middleware
    this.app.use(body_parser.urlencoded({ extended: true }));
    this.app.use(body_parser.json());
}

/**
 * @fn start
 * @desc Starts the OPAL scheduler service
 * @return {Promise} Resolves to a Express.js Application router on success,
 * rejects an error stack otherwise
 */
OpalScheduler.prototype.start = function() {
    let _this = this;
    return new Promise(function (resolve, reject) {
        _this._connectDb().then(function () {
            // Setup route using controllers
            _this._setupStatusController();

            // Setup the Helpers
            _this._setupMongoHelper();
            _this._setupSwiftHelper();

            // Setup the monitoring of the nodes' status
            _this._setupNodesWatchdog();

            // Setup the monitoring of jobs - Archive completed jobs, Invalidate timing out jobs
            _this._setupJobsWatchdog();

            // Setup the periodic scheduling of the jobs.
            _this._setupJobsScheduler();

            // Start status periodic update
            _this.status_helper.startPeriodicUpdate(5 * 1000); // Update status every 5 seconds

            // Start the monitoring of the nodes' status
            _this.nodes_watchdog.startPeriodicUpdate(60 * 1000); // Update status every 1 minute

            // Start the monitoring of the jobs' status
            _this.jobs_watchdog.startPeriodicUpdate(1800 * 1000); // Update status every 30 minutes

            // Start the scheduling of the queued jobs
            _this.jobs_scheduler.startPeriodicUpdate(1000); // Scheduling triggered every 1 second

            // :)
            _this.app.all('/whoareyou', function (_, res) {
                res.status(418);
                res.json(ErrorHelper('I\'m a teapot'));
            });

            // We take care of all remaining routes
            _this.app.all('/*', function (_, res) {
                res.status(400);
                res.json(ErrorHelper('Bad request'));
            });

            resolve(_this.app); // All good, returns application
        }, function (error) {
            reject(ErrorHelper('Cannot establish mongoDB connection', error));
        });
    });
};

/**
 * @fn stop
 * @desc Stop the OPAL scheduler service
 * @return {Promise} Resolves to a Express.js Application router on success,
 * rejects an error stack otherwise
 */
OpalScheduler.prototype.stop = function() {
    let _this = this;
    return new Promise(function (resolve, reject) {
        // Stop status update
        _this.status_helper.stopPeriodicUpdate();
        // Disconnect DB --force
        _this.db.close(true).then(function(error) {
            if (error)
                reject(ErrorHelper('Closing mongoDB connection failed', error));
            else
                resolve(true);
        });
    });
};

/**
 * @fn _connectDb
 * @desc Setup the connections with mongoDB
 * @return {Promise} Resolves to true on success
 * @private
 */
OpalScheduler.prototype._connectDb = function () {
    let _this = this;
    return new Promise(function (resolve, reject) {
        mongodb.connect(_this.config.mongoURL, {
            reconnectTries: 2000,
            reconnectInterval: 5000
        }, function (err, db) {
            if (err !== null && err !== undefined) {
                reject(ErrorHelper('Failed to connect to mongoDB', err));
                return;
            }
            _this.db = db;
            resolve(true);
        });
    });
};

/**
 * @fn _setupStatusController
 * @desc Initialize status service routes and controller
 * @private
 */
OpalScheduler.prototype._setupStatusController = function () {
    let _this = this;

    let statusOpts = {
        version: package_json.version
    };
    _this.status_helper = new StatusHelper(Constants.EAE_SERVICE_TYPE_SCHEDULER, global.opal_scheduler_config.port, null, statusOpts);
    _this.status_helper.setCollection(_this.db.collection(Constants.EAE_COLLECTION_STATUS));
    _this.status_helper.setStatus(Constants.EAE_SERVICE_STATUS_BUSY);

    _this.statusController = new StatusController(_this.status_helper);
    _this.app.get('/status', _this.statusController.getStatus); // GET status
    _this.app.get('/specs', _this.statusController.getFullStatus); // GET Full status
};

/**
 * @fn _setupMongoHelper
 * @desc Initialize the mongo helper
 * @private
 */
OpalScheduler.prototype._setupMongoHelper = function () {
    let _this = this;
    _this.mongo_helper.setCollections(_this.db.collection(Constants.EAE_COLLECTION_STATUS),
        _this.db.collection(Constants.EAE_COLLECTION_JOBS),
        _this.db.collection(Constants.EAE_COLLECTION_JOBS_ARCHIVE),
        _this.db.collection(Constants.EAE_COLLECTION_FAILED_JOBS_ARCHIVE));
};

/**
 * @fn _setupSwiftHelper
 * @desc Initialize the helper class to interact with Swift
 * @private
 */
OpalScheduler.prototype._setupSwiftHelper = function () {
    let _this = this;
    _this.swift_helper = new SwiftHelper({
        url: _this.config.swiftURL,
        username: _this.config.swiftUsername,
        password: _this.config.swiftPassword
    });
};

/**
 * @fn _setupNodesWatchdog
 * @desc Initialize the periodic monitoring of the compute nodes
 * @private
 */
OpalScheduler.prototype._setupNodesWatchdog = function () {
    let _this = this;
    _this.nodes_watchdog = new NodesWatchdog(_this.mongo_helper);
};

/**
 * @fn _setupJobsWatchdog
 * @desc Initialize the periodic monitoring of the Jobs
 * @private
 */
OpalScheduler.prototype._setupJobsWatchdog = function () {
    let _this = this;
    _this.jobs_watchdog = new JobsWatchdog(_this.mongo_helper, _this.swift_helper);
};

/**
 * @fn _setupJobsScheduler
 * @desc Initialize the periodic scheduling of the Jobs
 * @private
 */
OpalScheduler.prototype._setupJobsScheduler = function () {
    let _this = this;
    _this.jobs_scheduler = new JobsScheduler(_this.mongo_helper);
};


module.exports = OpalScheduler;
