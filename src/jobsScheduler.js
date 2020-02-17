const timer = require('timers');
const request = require('request');
const { Constants, ErrorHelper } =  require('eae-utils');

/**
 * @class JobsScheduler
 * @desc Periodic scheduling and managing of jobs - Job Statuses Managed: Queued, Error, Cancelled, Done
 * @param mongoHelper Helper class to interact with Mongo
 * @constructor
 */
function JobsScheduler(mongoHelper) {
    // Init member attributes
    this._mongoHelper = mongoHelper;

    //Bind member functions
    this.startPeriodicUpdate = JobsScheduler.prototype.startPeriodicUpdate.bind(this);
    this.stopPeriodicUpdate = JobsScheduler.prototype.stopPeriodicUpdate.bind(this);

    // Action Methods
    this._freeComputeResources = JobsScheduler.prototype._freeComputeResources.bind(this);
    this._reportFailedJob = JobsScheduler.prototype._reportFailedJob.bind(this);
    this._analyzeJobHistory = JobsScheduler.prototype._analyzeJobHistory.bind(this);
    this._queuedJobs = JobsScheduler.prototype._queuedJobs.bind(this);
    this._errorJobs = JobsScheduler.prototype._errorJobs.bind(this);
    this._canceledOrDoneJobs = JobsScheduler.prototype._canceledOrDoneJobs.bind(this);
}

/**
 * @fn _freeComputeResources
 * @desc Free all compute resources allocated to a specific job, e.g. workers of a spark cluster.
 * @param job Job to process
 * @private
 */
JobsScheduler.prototype._freeComputeResources = function(job){
    let _this = this;
    return new Promise(function(resolve, reject) {
        // Check if the job was running or not
        if(job.status[1] === Constants.EAE_JOB_STATUS_RUNNING &&
            (job.status[0] === Constants.EAE_JOB_STATUS_CANCELLED || job.status[0] === Constants.EAE_JOB_STATUS_ERROR)){
            request({
                    method: 'POST',
                    baseUrl: 'http://' + job.executorIP + ':' + job.executorPort,
                    uri: '/cancel'
                },
                function (error, response, _unused__body) {
                    if (error !== null) {
                        job.statusLock = false;
                        _this._mongoHelper.updateJob(job).then(
                            function(success_res){
                                if(success_res.nModified === 1) {
                                    reject(ErrorHelper('The cancel request has failed:', error));
                                }},function(error){
                                reject(ErrorHelper('Failed to unlock the job and cancel request failed:', error));
                            });
                        return;
                    }
                    // eslint-disable-next-line no-console
                    console.log('The cancel request sent to host ' + job.executorIP + ':' + job.executorPort
                        + ' and the response was ', response.statusCode);
                });
        }
        switch (job.type) {
            case Constants.EAE_JOB_TYPE_SPARK: {
                let filter = {
                    ip: job.executorIP,
                    port: job.executorPort
                };
                _this._mongoHelper.retrieveNodesStatus(filter).then(
                    function (node) {
                        let sparkCluster = node[0].clusters.spark;
                        sparkCluster.forEach(function (node) {
                            node.status = Constants.EAE_SERVICE_STATUS_IDLE;
                            node.statusLock = false;
                            _this._mongoHelper.updateNodeStatus(node);
                        });
                    },
                    function (error) {
                        reject(ErrorHelper('Failed to retrieve nodes status. Filter:' + filter.toString(), error));
                    }
                );
                resolve(true);
                break;
            }
            default: {
                resolve(true);
                break;
            }
        }
    });
};

/**
 * @fn _reportFailedJob
 * @desc We archive the failed job and check if the executor for the job exceeds the threshold for failed jobs
 * @param job Failed job
 * @private
 */
JobsScheduler.prototype._reportFailedJob = function(job){
    let _this = this;

    // We archive the failed job and check if the executor for the job exceeds the threshold for failed jobs
    _this._mongoHelper.archiveFailedJob(job).then(function () {
        const currentTime = new Date();

        let filter = {
            executorPort : job.executorPort,
            executorIP : job.executorIP,
            startDate: {
                '$ge': new Date(currentTime.setHours(currentTime.getDay() - 7))
            }
        };
        _this._mongoHelper.retrieveFailedJobs(filter).then(function(failedJobs){
            if(failedJobs.length > 3){
                let node = {
                    ip : job.executorIP,
                    port : job.executorPort,
                    status : Constants.EAE_SERVICE_STATUS_DEAD,
                    statusLock : true
                };
                // lock the node and set status to dead
                _this._mongoHelper.updateNodeStatus(node).then(
                    function(success){
                        if(success.nModified === 1){
                            // eslint-disable-next-line no-console
                            console.log('The node' + node.ip + ':' + node.port + 'has been set to DEAD successfully ' +
                                'following excessive job failures');
                        }else{
                            ErrorHelper('Something went horribly wrong when locking the node and setting to DEAD. Node: '
                                + node.ip + ' ' + node.port);
                        }},function(error){
                        ErrorHelper('Failed to lock the node and set its status to DEAD. Node: '
                            + node.ip + ' ' + node.port, error);
                    });
            }
        },function(error){
            ErrorHelper('Failed to retrieve failed jobs for executor:' + filter.toString(), error);
        });
    },function (error){
        ErrorHelper('Failed to archive failed Job. Job:' + job._id, error);
    });
};

/**
 * @fn _analyzeJobHistory
 * @desc Analyzes the history of the job, if the job exceeds we set it to DEAD otherwise nothing.
 * @param job Job to process
 * @returns {Promise} It resolves to true if the job exceeds the policy and false otherwise.
 * @private
 */

JobsScheduler.prototype._analyzeJobHistory = function (job) {
    let _this = this;
    return new Promise(function(resolve, reject) {
        let errorHistory = job.status.filter(function(it) {return it === Constants.EAE_JOB_STATUS_ERROR;});
        // This shouldn't happen!
        if(errorHistory.length > 3){
            reject(ErrorHelper('The number of errors in the job history exceeds 3!'));
        }
        // We set the job to DEAD if it finished in error three times
        if(errorHistory.length === 3){
            job.status.unshift(Constants.EAE_JOB_STATUS_DEAD);
            job.status.unshift(Constants.EAE_JOB_STATUS_COMPLETED);
            _this._mongoHelper.updateJob(job).then(function(){
                resolve(true);
            },function(error){
                reject(ErrorHelper('Failed to set the job to DEAD', error));
            });
        }else{
            resolve(false);
        }
    });
};

/**
 * @fn _queuedJobs
 * @desc Periodic processing of the Queued jobs. We first check if the jobs exceeds the number of reschedules authorized,
 * then we reserve all required resources for the job and finally the job is run.
 * @returns {Promise} Resolves to true if the job has been scheduled properly. False if no resource is available or
 * one worker in the cluster is not available.
 * @private
 */
JobsScheduler.prototype._queuedJobs = function () {
    let _this = this;
    return new Promise(function(resolve, reject) {
        let statuses = [Constants.EAE_JOB_STATUS_QUEUED];

        let filter = {
            'status.0': {$in: statuses},
            statusLock: false,
        };

        _this._mongoHelper.retrieveJobs(filter).then(function (jobs) {
            jobs.forEach(function (job) {
                // We set the lock
                job.statusLock = true;
                // lock the Job
                _this._mongoHelper.updateJob(job).then(
                    function (res) {
                        if (res.nModified === 1) {
                            _this._analyzeJobHistory(job).then(function(exceedsPolicy){
                                if(!exceedsPolicy){
                                    // Now we can start to schedule the job
                                    let filter = {
                                        status: Constants.EAE_SERVICE_STATUS_IDLE,
                                        computeType: job.type,
                                        statusLock: false
                                    };
                                    _this._mongoHelper.findAndReserveAvailableWorker(filter).then(
                                        function(candidateWorker){
                                            if(candidateWorker !== false && candidateWorker !== null){
                                                switch (job.type) {
                                                    case Constants.EAE_JOB_TYPE_SPARK: {
                                                        // We lock the cluster and set the candidate as the executor for the job
                                                        let reserved = [];
                                                        let updates = [];
                                                        candidateWorker.clusters.spark.forEach(function (workerNode) {
                                                            if (workerNode.statusLock === false) {
                                                                workerNode.statusLock = true;
                                                                updates.push(_this._mongoHelper.updateNodeStatus(workerNode));
                                                                reserved.push(workerNode);
                                                            }
                                                        });
                                                        Promise.all(updates).then(successes => {
                                                            let numberOfWorkersAcquired = successes.reduce((acc, curr) => curr.ok + acc, 0);
                                                            if (numberOfWorkersAcquired === candidateWorker.clusters.spark.length) {
                                                                candidateWorker.clusters.spark.forEach(function (workerNode) {
                                                                    workerNode.status = Constants.EAE_SERVICE_STATUS_BUSY;
                                                                    _this._mongoHelper.updateNodeStatus(workerNode).then(function () {
                                                                        },
                                                                        function (error) {
                                                                            reject(ErrorHelper('Error when setting node to busy ' +
                                                                                'in cluster. Node ' + candidateWorker.toString(), error));
                                                                        });
                                                                });
                                                            }
                                                            else {
                                                                // We free the reserved resources
                                                                reserved.forEach(function (reservedWorker) {
                                                                    reservedWorker.status = Constants.EAE_SERVICE_STATUS_IDLE;
                                                                    _this._mongoHelper.updateNodeStatus(reservedWorker).then(function () {
                                                                        },
                                                                        function (error) {
                                                                            reject(ErrorHelper('Error when setting node to busy ' +
                                                                                'in cluster. Node ' + reservedWorker.toString(), error));
                                                                        });
                                                                });
                                                                // we unlock the job
                                                                job.statusLock = false;
                                                                _this._mongoHelper.updateJob(job);
                                                                // eslint-disable-next-line no-console
                                                                console.log('No currently available resource for job : ' + job._id
                                                                    + ' of type ' + job.type + '.\nAt least one resource in the ' +
                                                                    'cluster is not available');
                                                                resolve(false);
                                                            }
                                                        }, reason => {
                                                            reject(ErrorHelper('Error when locking node in cluster: ' + reason));
                                                        });
                                                        break;
                                                    }
                                                    default:
                                                        // Nothing to do
                                                        break;
                                                }
                                                // Everything is set, we send the request to the worker
                                                job.statusLock = false;
                                                job.status.unshift(Constants.EAE_JOB_STATUS_SCHEDULED);
                                                _this._mongoHelper.updateJob(job).then(function () {
                                                    request({
                                                            method: 'POST',
                                                            baseUrl: 'http://' + candidateWorker.ip + ':' + candidateWorker.port,
                                                            uri:'/run',
                                                            json: true,
                                                            body: {
                                                                job_id: job._id.toHexString()
                                                            }
                                                        },
                                                        function (error, response, _unused__body) {
                                                            if (error !== null) {
                                                                reject(ErrorHelper('The run request has failed:', error));
                                                            }
                                                            // eslint-disable-next-line no-console
                                                            console.log('The run request sent to host ' + candidateWorker.ip
                                                                + ':' + candidateWorker.port + ' and the response was ', response.statusCode);
                                                            // We set the candidate as the executor for the job
                                                            job.executorIP = candidateWorker.ip;
                                                            job.executorPort = candidateWorker.port;
                                                            _this._mongoHelper.updateJob(job).then(function () {
                                                                resolve(true);
                                                            });

                                                        });
                                                });
                                            }else{
                                                // we unlock the job
                                                job.statusLock = false;
                                                _this._mongoHelper.updateJob(job);
                                                // eslint-disable-next-line no-console
                                                console.log('No currently available resource for job : ' + job._id
                                                    + ' of type ' + job.type);
                                                resolve(false);
                                            }
                                        },
                                        function (error) {
                                            reject(ErrorHelper('Failed to find and reserve a worker', error));
                                        }
                                    );
                                }else{
                                    // eslint-disable-next-line no-console
                                    console.log('The job ' + job._id + ' is now DEAD.');
                                    resolve(true);
                                }
                            },function(error){
                                reject(ErrorHelper('Could not analyze the job history. Error: ', error));
                            });
                        }
                    },function (error) {
                        reject(ErrorHelper('Failed to lock the job. Filter:' + job._id, error));
                    });
            });
        },function (error) {
            reject(ErrorHelper('Failed to retrieve Jobs. Filter:' + filter.toString(), error));
        });
    });
};

/**
 * @fn _errorJobs
 * @desc Periodic processing of the Jobs in error. We report the failed job & executor, free all resources and
 * queue again the job.
 * @returns {Promise}
 * @private
 */
JobsScheduler.prototype._errorJobs = function () {
    let _this = this;
    return new Promise(function(resolve, reject) {
        let statuses = [Constants.EAE_JOB_STATUS_ERROR];

        let filter = {
            'status.0': {$in: statuses},
            statusLock: false,
        };

        _this._mongoHelper.retrieveJobs(filter).then(function (jobs) {
            jobs.forEach(function (job) {
                // We set the lock
                job.statusLock = true;
                // lock the Job
                _this._mongoHelper.updateJob(job).then(
                    function (res) {
                        if (res.nModified === 1) {
                            // We report the failed executor and archive the failed job
                            _this._reportFailedJob(job);
                            // // We free all the compute resources
                            _this._freeComputeResources(job);
                            job.statusLock = false;
                            job.status.unshift(Constants.EAE_JOB_STATUS_QUEUED);
                            _this._mongoHelper.updateJob(job).then(function(success_res){
                                if(success_res.nModified === 1){
                                    resolve('The job in error has been successfully Queued and executor reported');
                                }else{
                                    reject(ErrorHelper('Something went terribly wrong when unlocking and Queueing job.' +
                                        ' JobId: ' + job._id));
                                }
                            },function(error){
                                reject(ErrorHelper('Failed to unlock the job ' + job._id + ' and set it back to Queued',
                                    error));
                            });
                        }else{
                            resolve('The Job in error ' + job._id.toString() + ' has already been processed.');
                        }
                    },
                    function (error) {
                        reject(ErrorHelper('Failed to lock the job. Filter:' + job._id, error));
                    });
            });
        },function (error) {
            reject(ErrorHelper('Failed to retrieve Jobs. Filter:' + filter.toString(), error));
        });
    });
};

/**
 * @fn _canceledOrDoneJobs
 * @desc Periodic processing of the Jobs in state cancelled or done.
 * We free all resources -- if the job was in RUNNING state -- and set the jobs to completed.
 * @returns {Promise}
 * @private
 */
JobsScheduler.prototype._canceledOrDoneJobs = function () {
    let _this = this;
    return new Promise(function(resolve, reject) {
        let statuses = [Constants.EAE_JOB_STATUS_CANCELLED, Constants.EAE_JOB_STATUS_DONE];

        let filter = {
            'status.0': {$in: statuses},
            statusLock: false,
        };

        _this._mongoHelper.retrieveJobs(filter).then(function (jobs) {
            jobs.forEach(function (job) {
                // We set the lock
                job.statusLock = true;
                // lock the Job
                _this._mongoHelper.updateJob(job).then(
                    function (res) {
                        // Check that we successfully locked the record
                        if (res.nModified === 1) {
                            //  We free all the compute resources allocated to compute job
                            _this._freeComputeResources(job).then(function (_unused__freed) {
                                job.statusLock = false;
                                job.status.unshift(Constants.EAE_JOB_STATUS_COMPLETED);
                                _this._mongoHelper.updateJob(job).then(function (success_res) {
                                    if (success_res.nModified === 1) {
                                        resolve('The job in error has been successfully set to completed and all resources freed.');
                                    } else {
                                        reject(ErrorHelper('Something went terribly wrong when unlocking and setting ' +
                                            'the job to completed. JobId: ' + job._id));
                                    }
                                }, function (error) {
                                    reject(ErrorHelper('Failed to unlock the job ' + job._id + ' and set it back to Queued',
                                        error));
                                });
                            }, function (error) {
                                reject(ErrorHelper('Impossible to free all the compute resources', error));
                            });
                        }}, function (error) {
                        reject(ErrorHelper('Failed to lock the job. Filter:' + job._id, error));
                    });
            }, function (error) {
                reject(ErrorHelper('Failed to retrieve Jobs. Filter:' + filter.toString(), error));
            });
        });
    });
};

/**
 * @fn startPeriodicUpdate
 * @desc Start an automatic scheduling, error processing and post processing of the Jobs
 * @param delay The intervals (in milliseconds) on how often to update the status
 */
JobsScheduler.prototype.startPeriodicUpdate = function(delay = Constants.STATUS_DEFAULT_UPDATE_INTERVAL) {
    let _this = this;

    //Stop previous interval if any
    _this.stopPeriodicUpdate();
    //Start a new interval update
    _this._intervalTimeout = timer.setInterval(function(){
        _this._queuedJobs(); // Schedule Pending Jobs
        _this._errorJobs(); // Reschedule failed Jobs
        _this._canceledOrDoneJobs(); // Post processing of cancelled and done Jobs
    }, delay);
};

/**
 * @fn stopPeriodicUpdate
 * @desc Stops the automatic update and synchronisation of the compute status of the nodes
 * Does nothing if the periodic update was not running
 */
JobsScheduler.prototype.stopPeriodicUpdate = function() {
    let _this = this;

    if (_this._intervalTimeout !== null && _this._intervalTimeout !== undefined) {
        timer.clearInterval(_this._intervalTimeout);
        _this._intervalTimeout = null;
    }
};

module.exports = JobsScheduler;
