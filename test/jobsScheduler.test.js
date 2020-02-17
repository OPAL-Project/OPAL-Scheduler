const { Constants } =  require('eae-utils');
let JobsSchedulerTestServer = require('./jobsSchedulerTestServer.js');

jasmine.DEFAULT_TIMEOUT_INTERVAL = 20000;

let jobsSchedulerTestServer = new JobsSchedulerTestServer();

beforeAll(() => {
    return jobsSchedulerTestServer.setup();
});

beforeEach(() => {
    return jobsSchedulerTestServer.dbCleanup();
});

afterAll(function ()  {
    return jobsSchedulerTestServer.shutdown();
});

test('_queued_jobs: If a queued job has failed 3 times, then it is set to dead and then completed', async () => {
    expect.assertions(2);

    let job = {
        status: [
            Constants.EAE_JOB_STATUS_QUEUED,
            Constants.EAE_JOB_STATUS_ERROR,
            Constants.EAE_JOB_STATUS_ERROR,
            Constants.EAE_JOB_STATUS_ERROR
        ],
        statusLock: false,
    };

    await jobsSchedulerTestServer.insertJob(job);

    await jobsSchedulerTestServer.jobsScheduler._queuedJobs();

    let jobs = await jobsSchedulerTestServer.mongo_helper.retrieveJobs({_id: job._id});
    expect(jobs[0].status[0]).toEqual(Constants.EAE_JOB_STATUS_COMPLETED);
    expect(jobs[0].status[1]).toEqual(Constants.EAE_JOB_STATUS_DEAD);
});

test('_errosJobs: A job in error state gets added to the archived collection and then queued again', async () => {
    expect.assertions(2);

    let job = {
        status: [Constants.EAE_JOB_STATUS_ERROR],
        statusLock: false,
    };

    await jobsSchedulerTestServer.insertJob(job);

    await jobsSchedulerTestServer.jobsScheduler._errorJobs();

    let jobs = await jobsSchedulerTestServer.mongo_helper.retrieveJobs({});
    expect(jobs[0].status).toEqual([Constants.EAE_JOB_STATUS_QUEUED, Constants.EAE_JOB_STATUS_ERROR]);

    let archived_jobs = await jobsSchedulerTestServer.mongo_helper.retrieveFailedJobs({_id: job._id});
    expect(archived_jobs.length).toBe(1);
});

test('_canceledOrDoneJobs: job in canceled state gets set to completed', async () => {
    let canceledJob = {
        status: [Constants.EAE_JOB_STATUS_CANCELLED],
        statusLock: false,
    };

    await jobsSchedulerTestServer.insertJob(canceledJob);

    await jobsSchedulerTestServer.jobsScheduler._canceledOrDoneJobs();

    let jobs = await jobsSchedulerTestServer.mongo_helper.retrieveJobs({});
    expect(jobs[0].status[0]).toEqual(Constants.EAE_JOB_STATUS_COMPLETED);
});

test('_canceledOrDoneJobs: job in done state gets set to completed', async () => {
    let doneJob = {
        status: [Constants.EAE_JOB_STATUS_DONE],
        statusLock: false,
    };

    await jobsSchedulerTestServer.insertJob(doneJob);

    await jobsSchedulerTestServer.jobsScheduler._canceledOrDoneJobs();

    let jobs = await jobsSchedulerTestServer.mongo_helper.retrieveJobs({});
    expect(jobs[0].status[0]).toEqual(Constants.EAE_JOB_STATUS_COMPLETED);
});


test('_queued_jobs: A queued non-spark job gets scheduled', async () => {
    expect.assertions(1);

    let job = {
        status: [Constants.EAE_JOB_STATUS_QUEUED],
        type: "r",
        executorIP: 'compute',
        executorPort: 80,
        statusLock: false
    };

    await jobsSchedulerTestServer.insertJob(job);

    await jobsSchedulerTestServer.jobsScheduler._queuedJobs();

    let jobs = await jobsSchedulerTestServer.mongo_helper.retrieveJobs({_id: job._id});
    expect(jobs[0].status).toContain(Constants.EAE_JOB_STATUS_SCHEDULED);
});

test('_queued_jobs: A queued spark job gets scheduled', async () => {
    expect.assertions(3);

    let node2 = {
        ip: "192.168.10.10",
        port: 80,
        status: Constants.EAE_SERVICE_STATUS_IDLE,
        statusLock: false
    };

    let node3 = {
        ip: "192.168.10.15",
        port: 80,
        status: Constants.EAE_SERVICE_STATUS_IDLE,
        statusLock: false
    };

    let node = {
        ip: "compute",
        port: 80,
        status: Constants.EAE_SERVICE_STATUS_IDLE,
        clusters: {
            spark: [node2, node3]
        },
        computeType: Constants.EAE_JOB_TYPE_SPARK,
        statusLock: false
    };

    let job = {
        status: [Constants.EAE_JOB_STATUS_QUEUED],
        statusLock: false,
        type: Constants.EAE_JOB_TYPE_SPARK,
    };

    await jobsSchedulerTestServer.insertJob(job);
    await jobsSchedulerTestServer.insertNode(node);
    await jobsSchedulerTestServer.insertNode(node2);
    await jobsSchedulerTestServer.insertNode(node3);

    await jobsSchedulerTestServer.jobsScheduler._queuedJobs();

    let nodes = await jobsSchedulerTestServer.mongo_helper.retrieveNodesStatus({_id: node._id});
    expect(nodes[0].status).toEqual(Constants.EAE_SERVICE_STATUS_LOCKED);

    nodes = await jobsSchedulerTestServer.mongo_helper.retrieveNodesStatus({_id: node2._id});
    expect(nodes[0].status).toEqual(Constants.EAE_SERVICE_STATUS_BUSY);

    nodes = await jobsSchedulerTestServer.mongo_helper.retrieveNodesStatus({_id: node3._id});
    expect(nodes[0].status).toEqual(Constants.EAE_SERVICE_STATUS_BUSY);
});
