const { Constants } =  require('eae-utils');
let MongoHelperTestServer = require('./mongoHelperTestServer.js');

jasmine.DEFAULT_TIMEOUT_INTERVAL = 10000;

let mongoHelperTestServer = new MongoHelperTestServer();

beforeEach(() => {
    return mongoHelperTestServer.setup();
});

afterAll(function ()  {
    return mongoHelperTestServer.shutdown();
});

test('It can insert and retrieve nodes', async () => {
    let node = {};

    await mongoHelperTestServer.mongo_helper._statusCollection.insertOne(node);
    let nodes = await mongoHelperTestServer.mongo_helper.retrieveNodesStatus({});

    expect(nodes.length).toBe(1);
    expect(nodes[0]).toEqual(node);
});

test('It can retrieve jobs with filtering options', async () => {
    let job1 = {
        executorIP: '1',
    };

    let job2 = {
        executorIP: '2',
    };

    await mongoHelperTestServer.mongo_helper._jobsCollection.insertOne(job1);
    await mongoHelperTestServer.mongo_helper._jobsCollection.insertOne(job2);
    let jobs = await mongoHelperTestServer.mongo_helper.retrieveJobs({executorIP: '1'});

    expect(jobs.length).toBe(1);
    expect(jobs[0]).toEqual(job1);
});

test('It can update a node', async () => {
    let node = {statusLock: false};

    await mongoHelperTestServer.mongo_helper._statusCollection.insertOne(node);

    node.statusLock = true;

    await mongoHelperTestServer.mongo_helper.updateNodeStatus(node);
    let nodes = await mongoHelperTestServer.mongo_helper.retrieveNodesStatus({});

    expect(nodes.length).toBe(1);
    expect(nodes[0].statusLock).toEqual(true);
});

test('It can update a job', async () => {
    let job = {executorIP: "1"};

    await mongoHelperTestServer.mongo_helper._jobsCollection.insertOne(job);

    job.executorIP = "2";

    await mongoHelperTestServer.mongo_helper.updateJob(job);
    let jobs = await mongoHelperTestServer.mongo_helper.retrieveJobs({});

    expect(jobs.length).toBe(1);
    expect(jobs[0].executorIP).toEqual("2");
});

test('Archiving a job removes it from the jobs collection and adds it to the archived jobs collection', async () => {
    job = {};
    await mongoHelperTestServer.mongo_helper._jobsCollection.insertOne(job);

    await mongoHelperTestServer.mongo_helper.archiveJob(job);
    let jobs = await mongoHelperTestServer.mongo_helper.retrieveJobs({});
    let archivedJobs = await mongoHelperTestServer.mongo_helper.retrieveArchivedJobs({});

    expect(jobs.length).toBe(0);
    expect(archivedJobs.length).toBe(1);
    expect(archivedJobs[0]).toEqual(job);
});

test('It can retrieve failed jobs with filtering options', async () => {
    let job = {
        executorIP: '1',
    };

    await mongoHelperTestServer.mongo_helper._failedJobsArchiveCollection.insertOne(job);
    let failedJobs = await mongoHelperTestServer.mongo_helper.retrieveFailedJobs({executorIP: '1'});

    expect(failedJobs.length).toBe(1);
    expect(failedJobs[0]).toEqual(job);
});

test('It can retrieve archived jobs with filtering options', async () => {
    let job = {
        executorIP: '1',
    };

    await mongoHelperTestServer.mongo_helper._jobsArchiveCollection.insertOne(job);
    let archivedJobs = await mongoHelperTestServer.mongo_helper.retrieveArchivedJobs({executorIP: '1'});

    expect(archivedJobs.length).toBe(1);
    expect(archivedJobs[0]).toEqual(job);
});

test('Archiving a failed job adds it to the failed jobs archive collection', async () => {
    job = {};
    await mongoHelperTestServer.mongo_helper._jobsCollection.insertOne(job);

    await mongoHelperTestServer.mongo_helper.archiveFailedJob(job);
    let jobs = await mongoHelperTestServer.mongo_helper.retrieveJobs({});
    let failedArchivedJobs = await mongoHelperTestServer.mongo_helper.retrieveFailedJobs({});

    expect(jobs.length).toBe(1);
    expect(jobs[0]).toEqual(job);
    expect(failedArchivedJobs.length).toBe(1);
    expect(failedArchivedJobs[0]).toEqual(job);
});

test('It can find and reserve an available worker', async () => {
    let node = {
        status: Constants.EAE_SERVICE_STATUS_IDLE,
        statusLock: false
    };

    await mongoHelperTestServer.mongo_helper._statusCollection.insertOne(node);

    await mongoHelperTestServer.mongo_helper.findAndReserveAvailableWorker({});

    let nodes = await mongoHelperTestServer.mongo_helper.retrieveNodesStatus({});

    expect(nodes.length).toBe(1);
    expect(nodes[0].status).toEqual(Constants.EAE_SERVICE_STATUS_LOCKED);
    expect(nodes[0].statusLock).toEqual(true);
});
