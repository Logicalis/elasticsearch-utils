/**
 *
 * BulkStream - a bulk stream for elasticsearch
 * (vhiroki - re91031z)
 *
 */

'use strict';

const stream = require('stream');

const BULK_THRESHOLD = 10000;
const BULK_PARALLEL_BULKS = 5;
const BULK_TIMEOUT_VALUE = 3000;

module.exports = class BulkStream extends stream.Writable {
    constructor(options) {
        super(options);

        this.esClient = options.esClient;

        this.actionsQueue = [];
        this.bulkTimeout = null;
    }
    _write(chunk, encoding, next) {
        const actionObjects = JSON.parse(chunk.toString());

        // add actions to queue
        actionObjects.forEach(ao => this.actionsQueue.push(ao));

        this.bulkTimeout && clearTimeout(this.bulkTimeout);

        if (this.actionsQueue.length > BULK_THRESHOLD) {
            this._startBulk();
        } else {
            this.bulkTimeout = setTimeout(() => {
                this._startBulk();
            }, BULK_TIMEOUT_VALUE);
        }

        next();
    }
    _startBulk() {
        let beingProcessedLenght = this.actionsQueue.length;
        this.cork();

        this.emit('bulk_started', beingProcessedLenght);
        this._processActions()
            .then(() => {
                this.emit('bulk_finished', beingProcessedLenght);
                this.uncork()
            }).catch(error => {
            console.log(error);
            this.end();
        });
    }
    _processActions() {
        const bulkArrays = [];
        while (this.actionsQueue.length > 0) {
            const actionsPartition = this.actionsQueue.splice(0, Math.ceil(BULK_THRESHOLD / BULK_PARALLEL_BULKS));
            const bulkArray = [];
            actionsPartition.forEach(action => {
                bulkArray.push(action.action);
                bulkArray.push(action.payload);
            });
            bulkArrays.push(bulkArray);
        }
        return Promise.all(bulkArrays.map(ba => this._writeToElastic(ba)));
    }
    _writeToElastic(bulkArray) {
        return this.esClient.bulk({
            body: bulkArray
        });
    }
};