const { ErrorHelper, Constants } =  require('eae-utils');

/**
 * @class MongoHelper
 * @desc Global mongo helper for the scheduler
 * @constructor
 */
function MongoHelper(){
    //Init member vars
    this._statusCollection = null;
    this._jobsCollection = null;
    this._jobsArchiveCollection = null;
    this._failedJobsArchiveCollection = null;

    //Bind member functions
    this.setCollections = MongoHelper.prototype.setCollections.bind(this);
    this.retrieveNodesStatus = MongoHelper.prototype.retrieveNodesStatus.bind(this);
    this.retrieveJobs = MongoHelper.prototype.retrieveJobs.bind(this);
    this.updateNodeStatus = MongoHelper.prototype.updateNodeStatus.bind(this);
    this.updateJob = MongoHelper.prototype.updateJob.bind(this);
    this.archiveJob = MongoHelper.prototype.archiveJob.bind(this);
    this.retrieveFailedJobs = MongoHelper.prototype.retrieveFailedJobs.bind(this);
    this.retrieveArchivedJobs = MongoHelper.prototype.retrieveArchivedJobs.bind(this);
    this.archiveFailedJob = MongoHelper.prototype.archiveFailedJob.bind(this);
    this.findAndReserveAvailableWorker = MongoHelper.prototype.findAndReserveAvailableWorker.bind(this);
}

/**
 * @fn setCollection
 * @desc Setup the mongoDB collection to sync against
 * @param statusCollection Initialized mongodb collection to work against for nodes' status
 * @param jobsCollection Initialized mongodb collection to work against for jobs processing
 * @param jobsArchiveCollection  Initialized mongodb collection to work against for archiving jobs
 * @param failedJobsArchiveCollection Initialized mongodb collection to work against for failed jobs
 */
MongoHelper.prototype.setCollections = function(statusCollection, jobsCollection, jobsArchiveCollection, failedJobsArchiveCollection) {
    this._statusCollection = statusCollection;
    this._jobsCollection = jobsCollection;
    this._jobsArchiveCollection = jobsArchiveCollection;
    this._failedJobsArchiveCollection = failedJobsArchiveCollection;
};

/**
 * @fn retrieveNodesWithStatus
 * @desc Retrieves the list of Nodes for the list of specified filter and projection.
 * @param filter
 * @param projection
 * @return {Promise} returns an array with all the nodes matching the desired status and projection
 */
MongoHelper.prototype.retrieveNodesStatus = function(filter, projection = {}){
    let _this = this;

    return new Promise(function(resolve, reject) {
        if (null === _this._statusCollection || undefined === _this._statusCollection) {
            reject(ErrorHelper('No MongoDB collection to retrieve the nodes statuses against'));
            return;
        }

        _this._statusCollection.find(filter, projection).toArray().then(function(docs) {
                resolve(docs);
            },function(error) {
                reject(ErrorHelper('Retrieve Nodes Status has failed', error));
            }
        );
    });
};

/**
 * @fn retrieveJobs
 * @desc Retrieves the list of Jobs
 * @param filter MongoDB filter for the query
 * @param projection MongoDB projection
 * @return {Promise} returns an array with all the jobs matching the desired status
 */
MongoHelper.prototype.retrieveJobs = function(filter, projection = {}){
    let _this = this;

    return new Promise(function(resolve, reject) {
        if (null === _this._jobsCollection || undefined === _this._jobsCollection) {
            reject(ErrorHelper('No MongoDB collection to retrieve the jobs against'));
            return;
        }

        _this._jobsCollection.find(filter, projection).toArray().then(function(docs) {
                resolve(docs);
            },function(error) {
                reject(ErrorHelper('Retrieve Jobs has failed', error));
            }
        );
    });
};

/**
 * @fn retrieveNodesWithStatus
 * @desc Retrieves the list of Nodes for the list of specified filter and projection.
 * @param node Node to be updated
 * @return {Promise} Resolve to mongo res if update operation has been successful
 */
MongoHelper.prototype.updateNodeStatus = function(node){
    let _this = this;

    return new Promise(function(resolve, reject) {
        if (null === _this._statusCollection || undefined === _this._statusCollection) {
            reject(ErrorHelper('No MongoDB collection to retrieve the nodes statuses against'));
            return;
        }

        let filter = { //Filter is based on ip/port combination
            ip: node.ip,
            port: node.port
        };

        _this._statusCollection.findOneAndUpdate(filter,
                                        { $set : node},
                                        { returnOriginal: true, w: 'majority', j: false })
            .then(function(res) {
                resolve(res);
            },function(error){
                reject(ErrorHelper('The update of the nodes status dead to locked has failed', error));
            }
        );
    });
};

/**
 * @fn updateJob
 * @desc Update the job for the specified filter and projection.
 * @param  job New json job to be inserted back to mongo.
 * @return {Promise} Resolve to the result of the update if update operation has been successful
 */
MongoHelper.prototype.updateJob = function(job){
    let _this = this;

    return new Promise(function(resolve, reject) {
        if (null === _this._jobsCollection || undefined === _this._jobsCollection) {
            reject(ErrorHelper('No MongoDB collection to retrieve the jobs against'));
            return;
        }

        let filter = {
            _id: job._id
        };

        _this._jobsCollection.updateOne(filter,
            { $set : job},
            { w: 'majority', j: false })
            .then(function(success) {
                    resolve(success.result);
                },function(error){
                    reject(ErrorHelper('The update of the job has failed', error));
                }
            );
    });
};

/**
 * @fn archiveJob
 * @desc transfer an expired job to the archive of jobs and purges swift.
 * @param jobId id of the job to be transferred to the archive.
 * @return {Promise} Resolve to the old job if the delete Job is successful
 */
MongoHelper.prototype.archiveJob = function(job){
    let _this = this;

    return new Promise(function(resolve, reject) {
        if (null === _this._jobsCollection || undefined === _this._jobsCollection ||
            null === _this._jobsArchiveCollection || undefined === _this._jobsArchiveCollection) {
            reject(ErrorHelper('Jobs and/or Archive collections in MongoDB is/are not accessible'));
            return;
        }

        let filter = { _id:  job._id };

        _this._jobsCollection.findOne(filter).then(function(job) {
                // delete job._id;
                _this._jobsArchiveCollection.insert(job).then(function(success) {
                        if (success.insertedCount === 1) {
                            _this._jobsCollection.deleteOne(filter).then(function(){
                                console.log('The job ' + job._id + 'has been successfully archived');// eslint-disable-line no-console
                                resolve(job);
                            },function(error){
                                reject(ErrorHelper('The old job could not be deleted properly from jobsCollection. ' +
                                    'JobID:' + job._id ,error));
                            });
                        }else{
                            reject(ErrorHelper('The job couldn\'t be inserted properly. The insert count != 1. ' +
                                'JobID:' + job._id));
                        }
                    },function(error){
                        reject(ErrorHelper('The job couldn\'t be inserted properly. The insert count != 1. ' +
                            'JobID:' + job._id, error));
                    }
                );
            },function(error){
                reject(ErrorHelper('The job couldn\'t be found JobID:' + job._id, error));
            }
        );
    });
};

/**
 * @fn retrieveFailedJobs
 * @desc Retrieves the list of failed Jobs
 * @param filter MongoDB filter for the query
 * @param projection MongoDB projection
 * @return {Promise} returns an array with all the jobs matching the desired status
 */
MongoHelper.prototype.retrieveFailedJobs = function(filter, projection = {}){
    let _this = this;

    return new Promise(function(resolve, reject) {
        if (null === _this._failedJobsArchiveCollection || undefined === _this._failedJobsArchiveCollection) {
            reject(ErrorHelper('No MongoDB collection to retrieve the failed jobs against'));
            return;
        }

        _this._failedJobsArchiveCollection.find(filter, projection).toArray().then(function(docs) {
                resolve(docs);
            },function(error) {
                reject(ErrorHelper('Retrieve failed Jobs has failed', error));
            }
        );
    });
};

/**
 * @fn retrieveArchivedJobs
 * @desc Retrieves the list of archived Jobs
 * @param filter MongoDB filter for the query
 * @param projection MongoDB projection
 * @return {Promise} returns an array with all the jobs matching the desired status
 */
MongoHelper.prototype.retrieveArchivedJobs = function(filter, projection = {}){
    let _this = this;

    return new Promise(function(resolve, reject) {
        if (null === _this._jobsArchiveCollection || undefined === _this._jobsArchiveCollection) {
            reject(ErrorHelper('No MongoDB collection to retrieve the failed jobs against'));
            return;
        }

        _this._jobsArchiveCollection.find(filter, projection).toArray().then(function(docs) {
                resolve(docs);
            },function(error) {
                reject(ErrorHelper('Retrieve failed Jobs has failed', error));
            }
        );
    });
};

/**
 * @fn archiveFailedJob
 * @desc Transfer an expired job to the archive of jobs and purges swift.
 * @param job Failed job to be saved to the archive.
 * @return {Promise} Resolve to true if the archiving of the Job is successful
 */
MongoHelper.prototype.archiveFailedJob = function(job){
    let _this = this;

    return new Promise(function(resolve, reject) {
        if (null === _this._failedJobsArchiveCollection || undefined === _this._failedJobsArchiveCollection) {
            reject(ErrorHelper('No MongoDB collection to retrieve the failed jobs against'));
            return;
        }

        // delete job._id;
        _this._failedJobsArchiveCollection.insert(job).then(function(success) {
            if (success.insertedCount === 1) {
                console.log('The failed job: ' + job._id + ' has been archived properly.'); // eslint-disable-line no-console
                resolve(true);
            }else{
                reject(ErrorHelper('The job couldn\'t be inserted properly. The insert count != 1. ' +
                    'Job:' + job.toString()));
            }},function(error){
            reject(ErrorHelper('The job couldn\'t be inserted properly. Job:' + job.toString(), error));
        });
    });
};

/**
 * @fn findAndReserveAvailableWorker
 * @desc Find an idle worker for the specific job type, then we set its status to reserved.
 * @param filter MongoDB filter for the query
 * @return {Promise} returns the candidate worker if there is any available, false otherwise.
 */
MongoHelper.prototype.findAndReserveAvailableWorker = function (filter) {
    let _this = this;

    return new Promise(function(resolve, reject) {
        if (null === _this._statusCollection || _this._statusCollection === undefined) {
            reject(ErrorHelper('No MongoDB collection to retrieve the nodes statuses against'));
            return;
        }
        let update = {
            status: Constants.EAE_SERVICE_STATUS_LOCKED,
            statusLock: true
        };

        _this._statusCollection.findOneAndUpdate(filter,
            { $set : update},
            { returnOriginal: true }).then(function(original){
                if(original.ok === 1){
                    resolve(original.value);
                }else{
                    reject(ErrorHelper('Something went horribly wrong when looking for an available resource'));
                }
            },function(error){
                reject(ErrorHelper('Failed to update node status. Node: ' + filter.toString(), error));
            }
        );
    });
};

module.exports = MongoHelper;
