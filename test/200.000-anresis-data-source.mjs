import section from 'section-tests';
import log from 'ee-log';
import assert from 'assert';
import SFTPServer from './lib/SFTPServer.mjs';
import AnresisDataSource from '../src/data-source/AnresisDataSource.mjs';



section('Anresis Data Source', (section) => {
    let server;


    section.setup('start the sftp server', async() => {
        server = new SFTPServer();
        await server.load();
        await server.listen();
    });


    section.test('Download a file', async() => {
        section.setTimeout(10000);


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
    });




    section.test('Get file hash', async() => {
        section.setTimeout(10000);


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
    });



    section.destroy(async() => {
        await server.end();
    });
});
