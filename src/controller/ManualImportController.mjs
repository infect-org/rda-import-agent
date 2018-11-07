import { Controller } from 'rda-service';
import type from 'ee-types';

/**
 * allows the user to manually trigger an import
 */
export default class ManualImportController extends Controller {

    constructor({
        importer,
    }) {
        super('manual-import');

        // the importer class that allows imports to be created
        this.importer = importer;

        this.enableAction('create');
    }







    /**
    * sets up a new cluster:
    * 1. check the cluster service about existing clusters
    * 2. get the data requirements from the data source
    * 3. request reasonably sized cluster at the cluster service
    * 4. tell the data source to create shards for the cluster
    * 5. tell the cluster to initialize
    * 6. enjoy!
    *
    * @param      {Express.request}   request   express request
    * @param      {Express.response}  response  express response
    * @return     {Promise}           object containing the clusters description
    */
    async create(request) {
        const data = await request.getData();
        const response = request.response();

        if (!data) {
            response.status(400).send('Missing request body!');
        } else if (!type.object(data)) {
            response.status(400).send('Request body must be an object!');
        } else if (!type.string(data.engine)) {
            response.status(400).send('Missing parameter \'engine\' in request body!');
        } else if (!type.string(data.data)) {
            response.status(400).send('Missing parameter \'data\' in request body!');
        } else if (!type.string(data.dataSet)) {
            response.status(400).send('Missing parameter \'dataSet\' in request body!');
        } else if (!this.importer.hasEndine(data.engine)) {
            response.status(400).send(`Engine ${data.engine} is not avilabe for exection!`);
        } else {
            await this.importer.import({
                importName: data.engine,
                dataSetName: data.dataSet,
                dataSourceOptions: data.config,
            });
        }
    }
}
