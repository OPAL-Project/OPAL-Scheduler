
module.exports = {
    mongoURL: 'mongodb://[username:password@]host1[:port1][,host2[:port2],...[,hostN[:portN]]][/[database][?options]]',
    port: 8080,
	enableCors: true,
    archivingEnabled: true,
    jobsExpiredStatusTime: 720 , // Time in hours: 24h * 30d. Jobs to be archived
    jobsTimingoutTime: 24 , // Time in hours: 24h. Jobs to be cancelled for exceeding computing time policy.
    nodesExpiredStatusTime: 10, // Time in seconds. Tolerance for the refresh of the nodes' status
    swiftURL: 'http://0.0.0.0:8080',
    swiftUsername: 'root',
    swiftPassword: 'root'
};
