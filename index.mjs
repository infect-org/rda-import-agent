import logd from 'logd';
import ConsoleTransport from 'logd-console-transport';
import Service from './src/Service.mjs';

// enable console logging
logd.transport(new ConsoleTransport());


export {
    Service as default,
};
