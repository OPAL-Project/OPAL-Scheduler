# OPAL-scheduler
[![Dim Sums](https://img.shields.io/badge/made-with_Dim_Sums-4da3ff.svg?style=flat-square)](https://www.opalproject.org)
[![Travis branch](https://img.shields.io/travis/OPAL-Project/OPAL-Scheduler/master.svg?style=flat-square)](https://travis-ci.org/OPAL-Project/OPAL-Scheduler) 
[![David](https://img.shields.io/david/OPAL-Project/opal-scheduler.svg?style=flat-square)](https://david-dm.org/OPAL-Project/opal-scheduler) 
[![David](https://img.shields.io/david/dev/OPAL-Project/opal-scheduler.svg?style=flat-square)](https://david-dm.org/OPAL-Project/opal-scheduler?type=dev) 

OPAL - Scheduler micro-service

---------------------------

The opal-scheduler service provides scheduling capabilities to the opal eco-system. While running jobs is handled by the opal-compute, 
managing and scheduling them is the role of opal-scheduler. 
To do so, the opal-scheduler runs continually in the background, checking periodically for jobs to be scheduled, queued or archived. 

We provide the [API documentation](doc-api-swagger.yml) in swagger 2.0 format. You can paste the content in the [swagger editor](http://editor.swagger.io/) to render the API documentation.

## Configuration
At its construction, the `opalScheduler` server receives a configuration object that MUST respect the following schema:
 * [Example configuration](config/opal.scheduler.sample.config.js)
 

### Supported Job States
 * `QUEUED` The job is in a queue and it is waiting to be scheduled
 * `SCHEDULED` The job has been scheduled
 * `RUNNING` The job is currently running
 * `DONE` The job has finished but it still need post processing
 * `COMPLETED` The job has finished and the post processing has been completed
 * `ERROR` The job encountered an error
 * `CANCELLED` The job has been cancelled
 * `DEAD` The job is dead

