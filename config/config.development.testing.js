const envr = require('envr');

module.exports = {
    registryHost: 'http://l.dns.porn:9000',
    schedules: [{
        source: 'anresis',
        schedule: '0, 30, */6 * * *',
        dataSetName: 'infect-test',
        config: {
            host: 'l.dns.porn',
            port: 2222,
            user: 'foo',
            file: 'share/test-data.csv.2',
        },
    }],
};
