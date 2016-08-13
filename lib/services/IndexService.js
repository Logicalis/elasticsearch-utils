'use strict';

const BulkStream = require('./../streams/BulkStream');
const ScrollerStream = require('./../streams/ScrollerStream');
const IndexStream = require('./../streams/IndexStream');
const Elasticsearch = require('elasticsearch');

module.exports = {
    copyDocs
};

/**
 * Copy all docs of given type from an index to another.
 * @param {Object} ops - Options object
 * @param {string} ops.fromIndex - Origin index name
 * @param {string} ops.toIndex - Destination index name
 * @param {string} ops.fromUrl - Origin elastic URL
 * @param {string} ops.toUrl - Destination elastic URL
 * @param {string} ops.type - type of docs
 */
function copyDocs(ops) {
    const fromIndex = ops.fromIndex;
    const toIndex = ops.toIndex;
    const type = ops.type;
    const fromUrl = ops.fromUrl;
    const toUrl = ops.toUrl;

    const fromEsClient = new Elasticsearch.Client({
        host: fromUrl || 'localhost:9200'
    });

    const toEsClient = (toUrl && toUrl !== fromUrl) ? new Elasticsearch.Client({
        host: toUrl
    }) : fromEsClient;

    const scrollerStream = new ScrollerStream({ 
        esClient: fromEsClient,
        index: fromIndex,
        type: type, 
        size: 500
    });
    
    const indexStream = new IndexStream({
        index: toIndex,
        type: type
    });
    
    const bulkStream = new BulkStream({
        esClient: toEsClient
    });

    scrollerStream
        .pipe(indexStream)
        .pipe(bulkStream);

    bulkStream.on('bulk_finished', (size) => {
        console.log(`${size} have been processed!`);
    });

    return new Promise((resolve, reject) => {
        bulkStream.on('finish', () => {
            //resolve();
        });
    });
}