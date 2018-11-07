import scheduler from 'node-schedule';
import logd from 'logd';


const log = logd.module('infect-rda-sheduler');



export default class Scheduler {


    constructor({
        schedules,
        importer,
    }) {
        this.schedules = schedules;
        this.importer = importer;

        this.scheduledJobs = [];
    }






    /**
     * cancels all jobs
     */
    end() {
        for (const job of this.scheduledJobs) {
            job.cancel();
        }
    }





    /**
     * set up the schedules
     */
    initialize() {
        for (const schedule of this.schedules) {

            log.info(`Scheduling job ${schedule.source} with the schedule ${schedule.schedule} ...`);

            const job = scheduler.scheduleJob(schedule.schedule, () => {
                this.runJob(schedule);
            });

            this.scheduledJobs.push(job);
        }
    }





    /**
     * used for testing, executes a specific job
     *
     * @param      {int}  index   The index
     */
    async runJobAtIndex(index, configOverride) {
        const config = Object.assign({}, this.schedules[index]);
        config.config = Object.assign({}, config.config, configOverride);

        return this.importer.import({
            importName: config.source,
            dataSetName: config.dataSetName,
            dataSourceOptions: config.config,
        });
    }




    /**
     * execute an scheduled job
     *
     * @param      {object}  config  schedule config
     */
    async runJob(config) {

        // run the job
        this.importer.import({
            importName: config.source,
            dataSetName: config.dataSetName,
            dataSourceOptions: config.config,
        }).then(() => {
            log.info(`The import ${config.source} was successful!`);
        }).catch((err) => {
            log.error(`The import ${config.source} failed:`);
            log.info(err);
        });
    }
}
