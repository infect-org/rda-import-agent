import SFTPClient from '../client/SFTPClient.mjs';



export default class AnresisDataSource {


    /**
     * let the outside know what our name is
     *
     * @return     {string}  The name.
     */
    static getName() {
        return 'anresis';
    }


    /**
     * set up the data source
     *
     * @param      {Object}  arg1             options
     * @param      {string}  arg1.file        path of the file on the server
     * @param      {string}  arg1.host        host to connect to
     * @param      {string}  arg1.privateKey  The privateKey
     * @param      {number}  arg1.port        The port
     * @param      {string}  arg1.user        The user
     */
    constructor({
        file,
        host,
        privateKey,
        port,
        user,
    }) {
        this.file = file;
        this.user = user;
        this.privateKey = privateKey;
        this.port = port;
        this.host = host;
    }




    /**
     * initialize all required functionality
     *
     * @return     {Promise}  undefined
     */
    async initialize() {
        this.client = new SFTPClient({
            user: this.user,
            privateKey: this.privateKey,
            port: this.port,
            host: this.host,
        });
    }




    /**
     * returns a stream containing the data to be imported
     *
     * @return     {Promise}  The stream.
     */
    async getStream() {
        return this.client.download(this.file);
    }





    /**
     * get the data as buffer. for testing purposes only.
     *
     * @return     {Promise}  The file.
     */
    async getFile() {
        const stream = await this.getStream();

        return new Promise((resolve, reject) => {
            let buffer;

            stream.on('data', (chunk) => {
                if (!buffer) buffer = chunk;
                else buffer += chunk;
            });

            stream.on('error', reject);
            stream.on('end', () => { console.log(buffer.toString());
                resolve(buffer);
            });
        });
    }





    /**
     * Gets the current import hash.
     *
     * @return     {Promise}  hash
     */
    async getCurrentImportHash() {
        return this.client.getCurrentImportHash(this.file);
    }
}
