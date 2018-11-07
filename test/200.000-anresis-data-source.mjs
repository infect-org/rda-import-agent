import section from 'section-tests';
import log from 'ee-log';
import assert from 'assert';
import SFTPServer from './lib/SFTPServer.mjs';
import AnresisDataSource from '../src/data-source/AnresisDataSource.mjs';



section('Anresis Data Source', (section) => {


    section.test('Download a file', async() => {
        section.setTimeout(10000);


        section.info(`starting sshd server`);
        const server = new SFTPServer();
        await server.load();
        await server.listen();


        const dataSource = new AnresisDataSource({
            host: 'l.dns.porn',
            port: 2222,
            user: 'foo',
            privateKey: server.privateKey,
            file: 'share/test-data.csv',
        });


        await dataSource.initialize();


        const file = await dataSource.getFile();

        assert(file);
        assert(file.length > 100);

        section.info(`stopping sshd server`);
        await server.end();
    });




    section.test('Get file hash', async() => {
        section.setTimeout(10000);


        section.info(`starting sshd server`);
        const server = new SFTPServer();
        await server.load();
        await server.listen();


        const dataSource = new AnresisDataSource({
            host: 'l.dns.porn',
            port: 2222,
            user: 'foo',
            privateKey: server.privateKey,
            file: 'share/test-data.csv',
        });


        await dataSource.initialize();


        const hash = await dataSource.getCurrentImportHash();

        assert(hash);
        assert.equal(hash.length, 64);

        section.info(`stopping sshd server`);
        await server.end();
    });
});
