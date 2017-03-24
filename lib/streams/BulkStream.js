const stream = require('stream');

// const BULK_PARALLEL_BULKS = 1;
const BULK_TIMEOUT_VALUE = 3000;

/**
 * Class representing an Elasticsearch Bulk stream.
 * @extends stream.Readable
 */
class BulkStream extends stream.Writable {
    /**
     * Create a BulkStream.
     * @param {object} options - Stream options.
     * @param {ElasticsearchClient} options.esClient - Elasticsearch client. More info: https://www.elastic.co/guide/en/elasticsearch/client/javascript-api/current/index.html
     */
    constructor(options) {
        super(options);
        const {esClient, size = 500} = options;
        this.esClient = esClient;
        this.bulkSize = size;
        this.actionObjectsQueue = [];

    }
    _write(chunk, encoding, next) {
        let actionObjects = JSON.parse(chunk.toString());
        if (!(actionObjects instanceof Array)) {
            actionObjects = [actionObjects];
        }
        this._processActionObjects(actionObjects)
            .then(next)
            .catch(err => {
                console.error('Error processing action objects.');
                console.error(err);
                next();
            });
    }
    _processActionObjects(actionObjects) {
        return new Promise((resolve, reject) => {
            const remainingSpaceInQueue = this.bulkSize - this.actionObjectsQueue.length;
            this.actionObjectsQueue = this.actionObjectsQueue.concat(actionObjects.splice(0, remainingSpaceInQueue));

            if (this.actionObjectsQueue.length >= this.bulkSize) {
                return this._writeActionObjectsToElastic(this.actionObjectsQueue)
                    .then(() => {
                        this.actionObjectsQueue = [];
                        if (actionObjects.length > 0) {
                            return this._processActionObjects(actionObjects);
                        }
                    })
                    .then(resolve)
                    .catch(reject);
            }

            resolve();
        });
    }
    _writeActionObjectsToElastic(actionObjects) {
        const bulkArray = actionObjects.reduce((bulkArray, {action, payload}) => {
            bulkArray.push(action);
            payload && bulkArray.push(payload);
            return bulkArray;
        }, []);

        console.log(`[BulkStream] Executing ${actionObjects.length} actions...`);
        return this.esClient.bulk({
            body: bulkArray
        }).then(() => {
            console.log(`[BulkStream] Executed ${actionObjects.length} actions!`);
        });
    }
}

module.exports = BulkStream;