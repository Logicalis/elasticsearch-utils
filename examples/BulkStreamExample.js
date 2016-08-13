'use strict';

const Elasticsearch = require('elasticsearch');
const stream = require('stream');
const BulkStream = require('../lib/streams/BulkStream');

class DummyDataGeneratorStream extends stream.Readable {
    constructor(options) {
        super(options);
        this.generatedCount = 0;
    }

    _read() {
        if (this.generatedCount >= 1000000000) {
            this.push(null);
        } else {
            this.generatedCount++;

            const doc = {
                name: 'dummy-name-' + this.generatedCount,
                age: this.generatedCount,
                department: 'dummy-department-' + this.generatedCount,
                curiosities: [
                    'dummy-name-1-' + this.generatedCount,
                    'dummy-name-2-' + this.generatedCount,
                    'dummy-name-3-' + this.generatedCount
                ],
                friends: Math.round(Math.random() * 100)
            };
            const action = {
                action: { index: { _index: 'dummy_index', _type: 'dummy_type' } },
                payload: doc
            };
            const actions = [action];
            this.push(JSON.stringify(actions));
        }
    }
}

const esClient = new Elasticsearch.Client({
    host: 'localhost:9200'
});

const bulkStream = new BulkStream({esClient: esClient});
const dummyDataGeneratorStream = new DummyDataGeneratorStream();

dummyDataGeneratorStream.pipe(bulkStream);

bulkStream.on('bulk_finished', (size) => {
    console.log(`${size} have been processed!`);
});