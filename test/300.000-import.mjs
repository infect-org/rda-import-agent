import section from 'section-tests';
import log from 'ee-log';
import SFTPServer from './lib/SFTPServer.mjs';
import ServiceManager from '@infect/rda-service-manager';
import Service from '../index.mjs';




section('Import', (section) => {
    let server;
    let sm;

    section.setup(async() => {
        sm = new ServiceManager({
            args: '--dev --subenv-testing --data-for-dev --log-level=error+ --log-module=*'.split(' '),
        });

        await sm.startServices('rda-service-registry');
        await sm.startServices('api', 'infect-rda-sample-importer', 'infect-rda-sample-storage');

        server = new SFTPServer();
        await server.load();
        await server.listen();
    });



    section.test('Execute the import', async() => {
        section.setTimeout(1200000);
        const service = new Service();
        await service.load();


        // Execute job 0
        const { scheduler } = service;
        await scheduler.runJobAtIndex(0, {
            privateKey: server.privateKey,
        });


        await section.wait(200);
        await service.end();
    });



    section.destroy(async() => {
        await sm.stopServices();
        await server.end();
    });
});
