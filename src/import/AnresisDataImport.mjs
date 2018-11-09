import crypto from 'crypto';
import csv from 'csv';
import HTTP2Client from '@distributed-systems/http2-client';
import log from 'ee-log';
import util from 'util';




const parse = util.promisify(csv.parse);


/**
 * imports samples delivered via CSV from anresis
 *
 * @class      Importer (name)
 */
export default class AnresisDataImport {


    /**
     * let the outside know what our name is
     *
     * @return     {string}  The name.
     */
    static getName() {
        return 'anresis';
    }



    /**
     * set up the importer
     *
     * @param      {string}  registryHost  The registry host
     */
    constructor({
        registryClient,
        dataSetName,
    }) {
        this.dataSetName = dataSetName;
        this.registryClient = registryClient;
        this.httpClient = new HTTP2Client();

        // how many records to import per page that is sent to the importer service
        this.pageSize = 10000;

        // how many requests can be sent to the import service in parallel
        this.maxThreads = 8;

        this.stats = {
            importedRecordCount: 0,
            duplicateRecordCount: 0,
            failedRecordCount: 0,
        };

        this.failedEntites = new Map();

        // when a data set is created for the first time, it needs to know which
        // fields are relevant to it. this are the current fields used by infect
        this.fields = [
            'ageGroupId',
            'antibioticId',
            'bacteriumId',
            'hospitalStatusId',
            'regionId',
            'resistance',
            'sampleDate',
        ];
    }




    /**
     * shut the class down
     */
    async end() {
        await this.httpClient.end();
    }





    /**
     * sets everything up so that the import of samples can begin
     *
     * @param      {string}   dataSetName  The data set name
     * @return     {Promise}  undefined
     */
    async initialize(sourceHash) {
        return this.createVersion(sourceHash);
    }





    /**
     * checks if a given hash was already imported
     *
     * @param      {string}   hash    The hash
     * @return     {Promise}  true if it was imported, false otherwise
     */
    async hashWasImported(hash) {
        const storageHost = await this.registryClient.resolve('infect-rda-sample-storage');
        const response = await this.httpClient.get(`${storageHost}/infect-rda-sample-storage.data-version/${hash}`)
            .query({
                dataSet: this.dataSetName,
            })
            .expect(200, 404)
            .send();

        return response.status(200);
    }




    /**
     * create a data version
     *
     * @param      {Object}   arg1          options
     * @return     {Promise}  undefined
     */
    async createVersion(sourceHash) {
        const importHost = await this.registryClient.resolve('infect-rda-sample-import');

        log.info(`creating data version for data set ${this.dataSetName} ...`);
        const response = await this.httpClient.post(`${importHost}/infect-rda-sample-import.anresis-import`)
            .expect(201)
            .send({
                dataSet: this.dataSetName,
                dataSetFields: this.fields,
                sourceHash,
            });

        const data = await response.getData();
        this.dataVersionId = data.id;
        return data.id;
    }






    /**
     * imports a set of rows passed as a csv string
     *
     * @param      {<type>}   rawCSVData  The raw csv data
     * @return     {Promise}  { description_of_the_return_value }
     */
    async importData(rawCSVData) {
        log.info(`parsing ${rawCSVData.length} characters of data ...`);
        let rows;

        try {
            rows = await parse(rawCSVData);
        } catch (err) {
            err.message = `Failed to parse CSV data: ${err.message}`;
            throw err;
        }

        // extract the relevant information from the rows, create a unique id
        // for them so that no row gets imported twice.
        rows = rows.map(row => this.getRecord(row));


        // build data pages, optimize for concurrent requests
        const threads = [];
        const rowCounts = rows.length;


        while (rows.length > 0) {

            // determine the page size to use
            const pageSize = (rows.length > (this.maxThreads * this.pageSize))
                ? this.pageSize
                : Math.ceil(rows.length / this.maxThreads);


            for (let i = 0, l = this.maxThreads; i < l; i++) {
                if (!threads[i]) threads[i] = [];
                threads[i].push(rows.slice(i * pageSize, (i + 1) * pageSize));
            }

            // remove all rows that were added to the threads
            rows = rows.slice(this.maxThreads * pageSize);
        }



        log.info(`importing ${rowCounts} rows using ${threads.length} threads ...`);
        await Promise.all(threads.map(async(recordSet) => {

            // import one set after another
            for (const setRows of recordSet) {
                await this.importPage(setRows);
            }
        }));
    }




    /**
     * activate the new data version so that it can be used
     *
     * @return     {Promise}  undefined
     */
    async activateDataVersion() {
        await this.updateDataVersion('active');
    }




    /**
     * update the data version to a given status
     *
     * @param      {string}   status  The status
     * @return     {Promise}  undefined
     */
    async updateDataVersion(status) {
        log.info(`changing data version status to '${status}' ...`);
        const storageHost = await this.registryClient.resolve('infect-rda-sample-storage');
        await this.httpClient.patch(`${storageHost}/infect-rda-sample-storage.data-version/${this.dataVersionId}`)
            .expect(200)
            .send({
                status,
            });
    }



    /**
     * fail the new data version
     *
     * @return     {Promise}  undefined
     */
    async failDataVersion() {
        await this.updateDataVersion('failed');
    }





    /**
     * normalizes one row of the csv file so that it can be sent to the importer service
     *
     * @param      {Array}   row     CSV row
     * @return     {Object}  the normalized record
     */
    getRecord(row) {
        /* eslint-disable max-len */
        // 0                                    1                           2               3                   4           5                   6               7           8                           9
        // sampleID                        ,    REGION_DESCRIPTION,         sample type,    type of origin,     age-group,  microorganism,      ABCLS_ACRONYM,  AB_NAME,    DELIVERED_QUALITATIVE_RES,  DAY_DAY
        // 95409B77F8E10B6437A3D819E6B0AAFB,    "Switzerland Nord-East",    urine,          outpatient,         45-64,      "Escherichia coli", "ceph4",        "Cefepime", s,                          08.12.2017
        //                                                                                                                                                                                              0123456789
        /* eslint-enable max-len */
        const sampleDate = `${row[9].substr(6, 4)}-${row[9].substr(3, 2)}-${row[9].substr(0, 2)}T00:00:00Z`;

        return {
            bacterium: row[5],
            antibiotic: row[7],
            ageGroup: row[4],
            region: row[1],
            sampleDate,
            resistance: row[8],
            hospitalStatus: row[3],
            sampleId: this.getHash(row[0], row[5], row[7], sampleDate),
        };
    }





    /**
     * creates a md5 hash from input strings
     *
     * @param      {(Array|string[])}  input   inputs
     * @return     {string}            The hash.
     */
    getHash(...input) {
        return crypto.createHash('md5').update(input.join('|') + Math.random()).digest('hex');
    }






    /**
     * imports a slice of records in one request to the imported
     *
     * @return     {Promise}  undefined
     */
    async importPage(rows) {
        log.debug(`importing ${rows.length} rows ...`);

        const start = Date.now();
        const importHost = await this.registryClient.resolve('infect-rda-sample-import');
        const response = await this.httpClient.patch(`${importHost}/infect-rda-sample-import.anresis-import/${this.dataVersionId}`)
            .expect(200)
            .send(rows);

        const responseData = await response.getData();

        log.info(`Imported ${responseData.importedRecordCount} records in ${Math.round((Date.now() - start) / 1000)} seconds, omitted ${responseData.duplicateRecordCount} duplicate records ...`);

        if (responseData.failedRecordCount > 0) {
            log.debug(`Failed to import ${responseData.failedRecordCount} records`);

            for (const record of responseData.failedRecords) {
                const prop = `${record.failedResource}.${record.failedProperty}`;
                if (!this.failedEntites.has(prop)) this.failedEntites.set(prop, new Map());

                const map = this.failedEntites.get(prop);
                if (!map.has(record.unresolvedValue)) map.set(record.unresolvedValue, 0);
                map.set(record.unresolvedValue, map.get(record.unresolvedValue) + 1);
            }
        }

        this.stats.failedRecordCount += responseData.failedRecordCount;
        this.stats.duplicateRecordCount += responseData.duplicateRecordCount;
        this.stats.importedRecordCount += responseData.importedRecordCount;
    }
}
