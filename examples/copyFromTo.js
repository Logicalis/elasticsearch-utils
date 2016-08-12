'use strict';

const Service = require('../lib/services/IndexService');

Service.copyDocs({
    fromIndex: 'origin_example_index',
    toIndex: 'destination_example_index',
    fromUrl: 'localhost:9200',
    toUrl: 'localhost:9200',
    type: 'type_example'
}).then(() => {
    console.log('Copy has finished!');
    process.exit(0);
}).catch(error => {
    console.error(error);
    process.exit(1);
});