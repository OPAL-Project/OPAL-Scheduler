const timer = require('timers');
const request = require('request');
const { ErrorHelper, Constants } =  require('eae-utils');

/**
 * @class JobsWatchdog
 * @desc Periodic monitoring of jobs - Archive completed jobs, Invalidate timing out jobs.
 * Job Statuses Managed: Scheduled, Running, Completed
 * @param mongoHelper Helper class to interact with Mongo
 * @param swiftHelper Helper class to interact with Swift
 * @constructor
 */
function JobsWatchdog(mongoHelper, swiftHelper) {
    //Init member vars
    this._intervalTimeout = null;
    this._mongoHelper = mongoHelper;
    this._swiftHelper = swiftHelper;

    //Bind member functions
    this.startPeriodicUpdate = JobsWatchdog.prototype.startPeriodicUpdate.bind(this);
    this.stopPeriodicUpdate = JobsWatchdog.prototype.stopPeriodicUpdate.bind(this);

    // Action Methods
    this._deleteSwiftFilesAndContainer = JobsWatchdog.prototype._deleteSwiftFilesAndContainer.bind(this);
    this._archiveJobs = JobsWatchdog.prototype._archiveJobs.bind(this);
    this._invalidateTimingOutJobs = JobsWatchdog.prototype._invalidateTimingOutJobs.bind(this);
}

/**
 * @fn _deleteSwiftFilesAndContainer
 * @desc Delete the files and hosting container in Swift.
 * @param container {String} name of the container -- _id of the job with either "_input" or "_output"
 * @param filesArray {String} names of the files to be deleted in the container
 * @private
 */
JobsWatchdog.prototype._deleteSwiftFilesAndContainer = function(container, filesArray) {
    let _this = this;

    if(Array.isArray(filesArray)) {
        let filesToBeDeleted = [];
        filesArray.forEach(function (file) {
            let d = _this._swiftHelper.deleteFile(container, file).then(
                function (_unused__deleteStatus) {
                    // eslint-disable-next-line no-console
                    console.log('File : ' + file + ' has been successfully deleted from container : ' + container);
                },
                function (error) {
                    ErrorHelper('Failed to delete file in swift: container - '
                        + container + ' file - ' + file, error);
                }
            );
            filesToBeDeleted.push(d);
        });

        Promise.all(filesToBeDeleted).then(function (_unused__array) {
            _this._swiftHelper.deleteContainer(container);
        }, function (error) {
            ErrorHelper('Failed to delete conatiner:' + container, error);
        });
    }
};

/**
 * @fn _archiveJob
 * @desc Periodic archiving of the jobs. Transfer the job record from the jobs collection to the archive collection,
 *  then it purges all input and output files from swift.
 * @returns {Promise}
 * @private
 */
JobsWatchdog.prototype._archiveJobs = function(){
    let _this = this;
    return new Promise(function(resolve, reject) {
        let statuses = [Constants.EAE_JOB_STATUS_COMPLETED];
        let currentTime = new Date().getTime();
        let archivingTime =  new Date(currentTime -  global.opal_scheduler_config.jobsExpiredStatusTime * 3600000);

        let filter = {
            'status.0': {$in: statuses},
            statusLock: false,
            endDate: {
                '$lt': archivingTime
            }
        };

        _this._mongoHelper.retrieveJobs(filter).then(function (jobs) {
            jobs.forEach(function (job) {
                // We set the lock
                job.statusLock = true;

                // lock the Job
                _this._mongoHelper.updateJob(job).then(
                    function (res) {
                        if(res.nModified === 1){
                            // We save the job id before archiving the job
                            let inputContainer = job._id + '_input';
                            let outputContainer = job._id + '_output';
                            // We archive the Job
                            _this._mongoHelper.archiveJob(job).then(function(job){
                                    // We purge the input and output files from Swift
                                    _this._deleteSwiftFilesAndContainer(inputContainer, job.input);
                                    _this._deleteSwiftFilesAndContainer(outputContainer, job.output);
                                    resolve('The job has been successfully archived and files removed from swift');
                                },
                                function (error){
                                    reject(ErrorHelper('Failed to archive the job: ' + job._id, error));
                                }
                            );
                        }else{
                            resolve('The job has already been archived');
                        }},
                    function (error) {
                        reject(ErrorHelper('Failed to lock the job. Job:' + job._id, error));
                    });
            });
        },function (error){
            reject(ErrorHelper('Failed to retrieve Jobs. Filter:' + filter.toString(), error));
        });
    });
};

/**
 * @fn _invalidateTimingOutJobs
 * @desc Inspects currently scheduled and running jobs and whether their start date is above the policy threshold
 * @returns {Promise}
 * @private
 */
JobsWatchdog.prototype._invalidateTimingOutJobs = function(){
    let _this = this;
    return new Promise(function(resolve, reject) {
        let statuses = [Constants.EAE_JOB_STATUS_SCHEDULED, Constants.EAE_JOB_STATUS_RUNNING];
        let currentTime = new Date().getTime();
        let timeOutTime =  new Date(currentTime -  global.opal_scheduler_config.jobsTimingoutTime * 3600000);
        let filter = {
            'status.0': {$in: statuses},
            statusLock: false,
            startDate: {
                '$lt': timeOutTime
            }
        };

        _this._mongoHelper.retrieveJobs(filter).then(function(jobs){
            jobs.forEach(function (job) {
                // We set the lock
                job.statusLock = true;

                // lock the Job
                _this._mongoHelper.updateJob(job).then(
                    function (res) {
                        if(res.nModified === 1){
                            request({
                                    method: 'POST',
                                    baseUrl: 'http://' + job.executorIP + ':' + job.executorPort,
                                    uri:'/cancel',
                                    json: true,
                                    body: {
                                        job_id: job._id.toHexString()
                                    }
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

                                    // We change the job status back to Queued and unlock the job for scheduling
                                    job.status.unshift(Constants.EAE_JOB_STATUS_QUEUED) ;
                                    job.statusLock = false;

                                    _this._mongoHelper.updateJob(job).then(
                                        function(success_res){
                                            if(success_res.nModified === 1){
                                                resolve('The timed out job has been successfully invalidated and Queued');
                                            }else{
                                                reject(ErrorHelper('Something went terribly wrong when unlocking and Queueing job. JobId: ' + job._id));
                                            }
                                        },function(error){
                                            reject(ErrorHelper('Failed to unlock the job ' + job._id + ' and set it back to Queued', error));
                                        }
                                    );
                                });
                        }else{
                            resolve('The Job ' + job._id.toString() + ' has already been timed out.');
                        }
                    },function(error){
                        reject(ErrorHelper('Failed to lock the job. Filter:' + job._id, error));
                    });
            });
        },function (error){
            reject(ErrorHelper('Failed to retrieve Jobs. Filter:' + filter.toString(), error));
        });
    });
};

/**
 * @fn startPeriodicUpdate
 * @desc Start an automatic archiving of jobs and invalidation of timed out jobs
 * @param delay The intervals (in milliseconds) on how often to update the status
 */
JobsWatchdog.prototype.startPeriodicUpdate = function(delay = Constants.STATUS_DEFAULT_UPDATE_INTERVAL) {
    let _this = this;

    //Stop previous interval if any
    _this.stopPeriodicUpdate();
    //Start a new interval update
    _this._intervalTimeout = timer.setInterval(function(){
        if(global.opal_scheduler_config.archivingEnabled) {
            _this._archiveJobs(); // Purge expired jobs
        }
        _this._invalidateTimingOutJobs(); // Invalidate jobs which have been for longer than a specified threshold
    }, delay);
};

/**
 * @fn stopPeriodicUpdate
 * @desc Stops the automatic archiving of jobs and invalidation of timed out jobs
 * Does nothing if the periodic update was not running
 */
JobsWatchdog.prototype.stopPeriodicUpdate = function() {
    let _this = this;

    if (_this._intervalTimeout !== null && _this._intervalTimeout !== undefined) {
        timer.clearInterval(_this._intervalTimeout);
        _this._intervalTimeout = null;
    }
};

module.exports = JobsWatchdog;
