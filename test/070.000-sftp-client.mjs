import section from 'section-tests';
import log from 'ee-log';
import assert from 'assert';
import SFTPServer from './lib/SFTPServer.mjs';
import SFTPClient from '../src/client/SFTPClient.mjs';



section('Docker SFTP Server', (section) => {



    section.test('Connect to the server', async() => {
        section.setTimeout(10000);


        section.info(`starting sshd server`);
        const server = new SFTPServer();
        await server.load();
        await server.listen();


        const client = new SFTPClient({
            host: 'l.dns.porn',
            port: 2222,
            user: 'foo',
            privateKey: server.privateKey,
        });


        section.info(`stopping sshd server`);
        await server.end();
    });



    section.test('Connect to the server', async() => {
        section.setTimeout(10000);


        section.info(`starting sshd server`);
        const server = new SFTPServer();
        await server.load();
        await server.listen();


        const client = new SFTPClient({
            host: 'l.dns.porn',
            port: 2222,
            user: 'foo',
            privateKey: server.privateKey,
        });


        const info = await client.getInfo('share/test-data.csv');

        assert(info);
        assert(info.size);
        assert(info.size > 100);

        section.info(`stopping sshd server`);
        await server.end();
    });



    section.test('download a file', async() => {
        section.setTimeout(10000);


        section.info(`starting sshd server`);
        const server = new SFTPServer();
        await server.load();
        await server.listen();


        const client = new SFTPClient({
            host: 'l.dns.porn',
            port: 2222,
            user: 'foo',
            privateKey: server.privateKey,
        });


        const stream = await client.download('share/test-data.csv');

        assert(stream);

        const data = await new Promise((resolve, reject) => {
            let data;

            stream.on('data', (chunk) => {
                if (!data) data = chunk;
                else data += chunk;
            });

            stream.on('error', reject);
            stream.on('end', () => {
                resolve(data);
            });
        });


        assert(data);
        assert.equal(data.toString().substr(0, 20), 'REGION_DESCRIPTION,s');

        section.info(`stopping sshd server`);
        await server.end();
    });
});
