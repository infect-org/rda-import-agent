import section from 'section-tests';
import log from 'ee-log';
import SFTPServer from './lib/SFTPServer.mjs';
import ServiceManager from '@infect/rda-service-manager';
import Service from '../index.mjs';




section('Import', (section) => {
    let sm;

    section.setup(async() => {
        sm = new ServiceManager({
            args: '--dev --subenv-testing --data-for-dev --log-level=error+ --log-module=*'.split(' '),
        });

        await sm.startServices('rda-service-registry');
        await sm.startServices('api', 'infect-rda-sample-importer', 'infect-rda-sample-storage');
    });



    section.test('Execute the import', async() => {
        section.setTimeout(30000);
        const service = new Service();

        await service.load();

        section.info(`starting sshd server`);
        const server = new SFTPServer();
        await server.load();
        await server.listen();


        // Execute job 0
        section.info(`importing test data`);
        const { scheduler } = service;
        await scheduler.runJobAtIndex(0, {
            privateKey: server.privateKey,
        });


        await section.wait(200);
        await service.end();
    });



    section.destroy(async() => {
        await sm.stopServices();
    });
});
