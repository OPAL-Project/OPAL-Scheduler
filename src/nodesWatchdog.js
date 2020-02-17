const timer = require('timers');
const { ErrorHelper, Constants } =  require('eae-utils');

/**
 * @class NodesWatchdog
 * @desc Compute nodes status watchdog. Use it to track the compute status of the nodes, purge expired status and invalidate dead nodes.
 * @param mongoHelper Helper class to interact with Mongo
 * @constructor
 */
function NodesWatchdog(mongoHelper) {
    //Init member vars
    this._intervalTimeout = null;
    this._mongoHelper = mongoHelper;

    //Bind member functions
    this.startPeriodicUpdate = NodesWatchdog.prototype.startPeriodicUpdate.bind(this);
    this.stopPeriodicUpdate = NodesWatchdog.prototype.stopPeriodicUpdate.bind(this);

    // Action Methods
    this._notifyAdmin = NodesWatchdog.prototype._notifyAdmin.bind(this);
    this._excludeNodes = NodesWatchdog.prototype._excludeNodes.bind(this);
    this._invalidateDead = NodesWatchdog.prototype._invalidateDead.bind(this);
    this._purgeExpired = NodesWatchdog.prototype._purgeExpired.bind(this);
}

/**
 * @fn _notifyAdmin
 * @desc Sends a mail to the Administrator with the nodes which are in DEAD status
 * @param {JSON} deadNode
 * @private
 */
NodesWatchdog.prototype._notifyAdmin = function(deadNode){
    // For now it just prints to the console but in the future we want it to send a mail. #TODO
    // eslint-disable-next-line no-console
    console.log('The node with IP ' + deadNode.ip + ' and port ' + deadNode.port);
};

/**
 * @fn _excludeNodes
 * @desc Set the status of the dead nodes to excluded
 * @param {Array} deadNodes List of dead nodes
 * @private
 */
NodesWatchdog.prototype._excludeNodes = function(deadNodes){
    let _this = this;

    return new Promise(function(resolve, reject) {
        deadNodes.forEach(function (node) {
            node.statusLock = true;
            _this._mongoHelper.updateNodeStatus(node).then(function (success) {
                _this._notifyAdmin(node);
                resolve(success);
            }, function (error) {
                reject(ErrorHelper('Failed to update the nodes status to dead. Node: ' + node.ip + ' ' + node.port, error));
            });
        });
    });
};

/**
 * @fn _purgeExpired
 * @desc Remove all Nodes that have been Busy or Reserved for longer than a defined threshold
 * @private
 */
NodesWatchdog.prototype._purgeExpired = function() {
    let _this = this;

    return new Promise(function(resolve, reject) {
        let statuses = [Constants.EAE_SERVICE_STATUS_BUSY, Constants.EAE_SERVICE_STATUS_LOCKED];
        let currentTime = new Date().getTime();
        let nodesTimeout = new Date(currentTime -  global.opal_scheduler_config.nodesExpiredStatusTime * 1000);

        let filter = {
            status: {$in: statuses},
            statusLock: false,
            lastUpdate: {
                '$lt': nodesTimeout
            }
        };
        _this._mongoHelper.retrieveNodesStatus(filter).then(function (nodes) {
            nodes.forEach(function (node) {
                node.status = Constants.EAE_SERVICE_STATUS_DEAD;
                node.statusLock = true;

                // lock the node
                _this._mongoHelper.updateNodeStatus(node).then(
                    function (success) {
                        if (success.nModified === 1) {
                            // eslint-disable-next-line no-console
                            console.log('The expired node' + node.ip + ':' + node.port + 'has been set to DEAD successfully');
                            resolve(success);
                        } else {
                            // eslint-disable-next-line no-console
                            console.log('The node has already been updated ' + node.ip + ':' + node.port);
                            resolve(success);
                        }
                    },
                    function (error) {
                        reject(ErrorHelper('Failed to lock the node and set its status to DEAD. Node: ' + node.ip + ' ' + node.port, error));
                    });
            });
            resolve();
        }, function (error) {
            reject(ErrorHelper('Failed to retrieve nodes status. Filter:' + filter.toString(), error));
        });
    });
};

/**
 * @fn _invalidateDead
 * @desc Remove nodes whose status is Dead from available resources
 * @return
 * @private
 */
NodesWatchdog.prototype._invalidateDead = function() {
    let _this = this;
    return new Promise(function(resolve, reject) {
        let statuses = [Constants.EAE_SERVICE_STATUS_DEAD];

        let filter = {
            status: {$in: statuses},
            statusLock: false
        };

        _this._mongoHelper.retrieveNodesStatus(filter).then(function (deadNodes) {
            if (deadNodes.length > 0) {
                _this._excludeNodes(deadNodes).then(function(success) {
                    resolve(success);
                }, function(error) {
                    reject(error);
                });
            }
        }, function (error) {
            // eslint-disable-next-line no-console
            reject(ErrorHelper('Failed to retrieve nodes status. Filter:' + filter.toString(), error));
        });
    });
};

/**
 * @fn startPeriodicUpdate
 * @desc Start an automatic update and synchronisation of the compute status of the nodes
 * @param delay The intervals (in milliseconds) on how often to update the status
 */
NodesWatchdog.prototype.startPeriodicUpdate = function(delay = Constants.STATUS_DEFAULT_UPDATE_INTERVAL) {
    let _this = this;

    //Stop previous interval if any
    _this.stopPeriodicUpdate();
    //Start a new interval update
    _this._intervalTimeout = timer.setInterval(function(){
        _this._purgeExpired(); // Purge expired jobs
        _this._invalidateDead(); // Purge dead nodes
    }, delay);
};

/**
 * @fn stopPeriodicUpdate
 * @desc Stops the automatic update and synchronisation of the compute status of the nodes
 * Does nothing if the periodic update was not running
 */
NodesWatchdog.prototype.stopPeriodicUpdate = function() {
    let _this = this;

    if (_this._intervalTimeout !== null && _this._intervalTimeout !== undefined) {
        timer.clearInterval(_this._intervalTimeout);
        _this._intervalTimeout = null;
    }
};

module.exports = NodesWatchdog;
