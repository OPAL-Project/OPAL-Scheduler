const { Constants } =  require('eae-utils');
let JobsWatchdogTestServer = require('./jobsWatchdogTestServer.js');

jasmine.DEFAULT_TIMEOUT_INTERVAL = 20000;

let jobsWatchdogTestServer = new JobsWatchdogTestServer();

beforeEach(() => {
    return jobsWatchdogTestServer.setup();
});

afterAll(function ()  {
    return jobsWatchdogTestServer.shutdown();
});

test('Expired completed jobs are archived', async () => {
    let job = {
        status: [
            Constants.EAE_JOB_STATUS_COMPLETED
        ],
        endDate: new Date(Date.now() - 8640000), // Yesterday date
        statusLock: false,
    };

    await jobsWatchdogTestServer.insertJob(job);
    await jobsWatchdogTestServer.jobsWatchdog._archiveJobs();
    let jobs = await jobsWatchdogTestServer.mongo_helper.retrieveJobs({});
    let archived_jobs = await jobsWatchdogTestServer.mongo_helper.retrieveArchivedJobs({});

    expect(jobs.length).toBe(0);
    expect(archived_jobs[0]._id).toEqual(job._id);
});

test('Running jobs which have timed out are queued again', async () => {
    let job = {
        status: [
            Constants.EAE_JOB_STATUS_RUNNING
        ],
        executorIP: 'compute',
        executorPort: 80,
        startDate: new Date(Date.now() - 8640000), // Yesterday date
        statusLock: false,
    };

    await jobsWatchdogTestServer.insertJob(job);
    await jobsWatchdogTestServer.jobsWatchdog._invalidateTimingOutJobs();
    let jobs = await jobsWatchdogTestServer.mongo_helper.retrieveJobs({});

    expect(jobs[0].status).toEqual([Constants.EAE_JOB_STATUS_QUEUED, Constants.EAE_JOB_STATUS_RUNNING]);
});

test('Scheduled jobs which have timed out are queued again', async () => {
    let job = {
        status: [
            Constants.EAE_JOB_STATUS_SCHEDULED
        ],
        executorIP: 'compute',
        executorPort: 80,
        startDate: new Date(Date.now() - 8640000), // Yesterday date
        statusLock: false,
    };

    await jobsWatchdogTestServer.insertJob(job);
    await jobsWatchdogTestServer.jobsWatchdog._invalidateTimingOutJobs();
    let jobs = await jobsWatchdogTestServer.mongo_helper.retrieveJobs({});

    expect(jobs[0].status).toEqual([Constants.EAE_JOB_STATUS_QUEUED, Constants.EAE_JOB_STATUS_SCHEDULED]);
});
