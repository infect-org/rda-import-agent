import SFTPClient from '@distributed-systems/sftp-client';
import Client from './Client.mjs';


/**
 * simple ftp client for downloading files and getting info
 *
 * @class      FTPDownloader (name)
 */
export default class FTPDownloader extends Client {

    constructor({
        host,
        user,
        privateKey,
        port,
    }) {
        super();

        this.port = port;
        this.user = user;
        this.host = host;
        this.privateKey = privateKey;
    }




    /**
     * get size and last modification date for a file
     *
     * @param      {string}   file    path to file
     * @return     {Promise}  info object
     */
    async getInfo(file) {
        const client = await this.getClient();
        const stats = await client.stat(file);
        await client.end();

        return {
            size: stats.size,
            modificationDate: new Date(stats.mtime),
        };
    }



    /**
     * download a file
     *
     * @param      {string}   file    file path
     * @return     {Promise}  readable stream
     */
    async download(file) {
        const client = await this.getClient();
        const readStream = await client.createReadStream(file);

        // make sure the client is closed when not used anymore
        readStream.on('error', () => {
            client.end();
        });

        readStream.on('close', () => {
            client.end();
        });

        return readStream;
    }




    /**
     * get a ftp client
     */
    async getClient() {
        const client = new SFTPClient();

        await client.connect({
            host: this.host,
            username: this.user,
            privateKey: this.privateKey,
            port: this.port,
        });

        return client;
    }
}
