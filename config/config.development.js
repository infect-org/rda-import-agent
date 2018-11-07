const envr = require('envr');

module.exports = {
    registryHost: 'http://l.dns.porn:9000',
    schedules: [{
        source: 'anresis',
        schedule: '0, 30, */6 * * *',
        dataSetName: 'infect',
        config: {
            host: envr.get('anresis-host'),
            port: 22,
            user: 'infect',
            password: envr.get('anresis-password'),
            file: '/upload/INFECT_export_month.csv',
        },
    }],
};
