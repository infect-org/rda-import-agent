import RDAService from 'rda-service';
import ManualImportController from './controller/ManualImportController.mjs';
import Importer from './Importer.mjs';
import Scheduler from './Scheduler.mjs';





export default class RDAImportAgentService extends RDAService {


    constructor() {
        super('import-agent');
    }




    /**
     * shut the service down
     *
     * @return     {Promise}  undefined
     */
    async end() {
        this.scheduler.end();
        await super.end();
    }




    /**
    * prepare the service
    */
    async load() {

        // the importer manages all data sources and data sinks
        this.importer = new Importer({
            registryHost: this.config.registryHost,
        });


        // the scheduler triggers imports as configured in the config
        this.scheduler = new Scheduler({
            schedules: this.config.schedules,
            importer: this.importer,
        });


        this.registerController(new ManualImportController({
            importer: this.importer,
        }));

        // load the web server
        await super.load();

        // tell the service registry that we're up and running
        await this.registerService();

        // schedule all configured jobs
        this.scheduler.initialize();
    }
}
