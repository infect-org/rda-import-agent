import section from 'section-tests';
import log from 'ee-log';
import ServiceManager from '@infect/rda-service-manager';
import Service from '../index.mjs';




section('INFECT Sample Importer for RDA', (section) => {
    let sm;

    section.setup(async() => {
        sm = new ServiceManager({
            args: '--dev --log-level=error+ --log-module=*'.split(' '),
        });

        await sm.startServices('rda-service-registry');
    });



    section.test('Start & stop service', async() => {
        const service = new Service();

        await service.load();
        await section.wait(200);
        await service.end();
    });



    section.destroy(async() => {
        await sm.stopServices();
    });
});
