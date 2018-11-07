import Client from './Client.mjs';
import SFTPReadStream from './SFTPReadStream.mjs';
import ssh2 from 'ssh2';
const SFTPClient = ssh2.Client;


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
        const stats = await new Promise((resolve, reject) => {
            client.stat(file, (err, stats) => {
                if (err) reject(err);
                else resolve(stats);
            });
        });
        client.end();

        return {
            size: stats.size,
            modificationDate: new Date(stats.modifyTime),
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

        // see https://github.com/jyu213/ssh2-sftp-client/issues/37
        // a quality piece of software! angry lina is ranting
        const readStream = await client.createReadStream(file, {
            highWaterMark: 65535,
        });


        // make sure the client is closed when not used anymore
        readStream.on('error', () => {console.log('error');
            client.end();
        });

        readStream.on('data', () => {console.log('data');

        });

        readStream.on('end', () => {console.log('end');
            client.end();
        });

        //readStream.resume();

        return readStream; //new SFTPReadStream(readStream);
    }




    /**
     * get a ftp client
     */
    async getClient() {
        const client = new SFTPClient();

        return new Promise((resolve, reject) => {
            client.on('error', reject);
            client.on('ready', () => {

                // get the sftp channel
                client.sftp((err, sftpClient) => {
                    if (err) reject(err);
                    else resolve(sftpClient);
                });
            });

            client.connect({
                host: this.host,
                username: this.user,
                privateKey: this.privateKey,
                port: this.port,
            });
        });
    }
}
