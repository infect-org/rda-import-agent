import log from 'ee-log';
import RegistryClient from '@infect/rda-service-registry-client';
import AnresisDataImport from './import/AnresisDataImport.mjs';
import AnresisDataSource from './data-source/AnresisDataSource.mjs';



/**
 * factory class for importers
 */
export default class Importer {


    constructor({
        registryHost,
    }) {
        this.registryClient = new RegistryClient(registryHost);

        // options passed to importers
        this.options = {
            registryClient: this.registryClient,
        };

        this.importers = new Map([
            [AnresisDataImport.getName(), AnresisDataImport],
        ]);

        this.dataSources = new Map([
            [AnresisDataSource.getName(), AnresisDataSource],
        ]);
    }





    /**
     * import data from a data source into a import target for a specific data set
     *
     * @param      {Object}   arg1                    options
     * @param      {string}   arg1.importName         the name of the import to use for storing data
     * @param      {string}   arg1.dataSetName        The data set name
     * @param      {object}   arg1.dataSourceOptions  The data source options
     * @return     {Promise}  undefined
     */
    async import({
        importName,
        dataSetName,
        dataSourceOptions,
    }) {
        const importer = this.createImport(importName, dataSetName);
        const dataSource = await this.createDataSource(importName, dataSourceOptions);
        const currentHash = await dataSource.getCurrentImportHash();
        const hashWasImported = await importer.hashWasImported(currentHash);

        if (!hashWasImported) {

            // run the import since it has not been executed
            // on the current data received from the data source
            await this.executeImport({
                importer,
                dataSource,
            });
        }
    }





    /**
     * run the actual import
     *
     * @param      {Object}   arg1             options
     * @param      {object}   arg1.importer    The importer
     * @param      {object}   arg1.dataSource  The data source
     * @return     {Promise}  undefined
     */
    async executeImport({
        importer,
        dataSource,
    }) {

        // create a data version
        await importer.initialize();

        const stream = await dataSource.getStream();

        // import the data in chunks. wrap it in a promise since the stream
        // doesn't support promises natively
        await new Promise((resolve, reject) => {
            let isFirstChunk = true;
            let data;

            // if there happens an error inside the data event of the stream, it is 
            // passed ti the stream.destroy method. that method triggers the close event
            // before the error event. thats the reason we have manually to check if an 
            // error happend in the close event, sothat no data is processed there anymore.
            let streamError = false;


            stream.on('data', (chunk) => {

                // pause until we've ingested the data
                stream.pause();

                if (!data) data = chunk;
                else data = Buffer.concat([data, chunk]);

                // get all lines up to the last line break, import them, store the rest
                // in the data buffer
                const lines = data.toString().split('\n');
                let importableLines = lines.slice(0, lines.length - 1);
                data = Buffer.from(lines[lines.length - 1]);

                // remove the first row, it contains the labels
                if (isFirstChunk) {
                    isFirstChunk = false;
                    importableLines = importableLines.slice(1);
                }

                importer.importData(importableLines.join('\n')).then(() => {

                    // continue consuming data
                    stream.resume();
                }).catch((err) => {
                    streamError = true;

                    // kill the stream, we cannot recover from this!
                    err.message = `Failed to import data, aborting import: ${err.message}`;
                    stream.destroy(err);
                });
            });


            stream.on('close', () => {

                // do the normal data processing only if there weren't any errors
                if (!streamError) {

                    // ingest the remaining data
                    importer.importData(data.toString()).then(resolve).catch((err) => {

                        // kill the stream, we cannot recover from this!
                        err.message = `Failed to import data, aborting import: ${err.message}`;
                        reject(err);
                    });
                }
            });


            stream.on('error', (err) => {
                streamError = true;
                reject(err);
            });
        }).catch(async(err) => {
            await importer.failDataVersion().catch((secondErr) => {
                err.message = `While failing the data version because the stream failed: ${secondErr.message}. Original error: ${err.message}`;
                throw err;
            });
        });


        // done!
        await importer.activateDataVersion();
    }






    /**
     * Determines if the given importer exists
     *
     * @param      {string}  importName  The import name
     */
    hasImport(importName) {
        return this.importers.has(importName);
    }



    /**
     * creates an importer instance, returns it
     *
     * @param      {string}    importName  The import name
     * @return     {importer}  instance of the requested importer
     */
    createImport(importName, dataSetName) {
        if (this.hasImport(importName)) {
            const Constructor = this.importers.get(importName);
            const instance = new Constructor(Object.assign({ dataSetName }, this.options));
            return instance;
        } else {
            throw new Error(`Importer '${importName}' does not exist!`);
        }
    }






    /**
     * Determines if the given data source exists
     *
     * @param      {string}  dataSourceName  The data source name
     */
    hasDataSource(dataSourceName) {
        return this.dataSources.has(dataSourceName);
    }



    /**
     * creates an data source instance, returns it
     *
     * @param      {string}    dataSourceName  The data source name
     * @return     {importer}  instance of the requested importer
     */
    async createDataSource(dataSourceName, options) {
        if (this.hasDataSource(dataSourceName)) {
            const Constructor = this.dataSources.get(dataSourceName);
            const instance = new Constructor(options);

            await instance.initialize();

            return instance;
        } else {
            throw new Error(`Data source '${dataSourceName}' does not exist!`);
        }
    }
}
