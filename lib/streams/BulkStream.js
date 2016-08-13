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
const BULK_TIMEOUT_VALUE = 5000;

module.exports = class BulkStream extends stream.Writable {
    constructor(options) {
        super(options);
        this.esClient = options.esClient;
        this.actionsBuffer = [];

        //this.bulkTimeout = null;

        this._processBuffer = this._processBuffer.bind(this);
    }
    _write(chunk, encoding, next) {
        const actionObjects = JSON.parse(chunk.toString());

        this.actionsBuffer = this.actionsBuffer.concat(actionObjects);

        if (this.actionsBuffer.length < BULK_THRESHOLD) {
            /*
            this.bulkTimeout && clearTimeout(this.bulkTimeout);
            this.bulkTimeout = setTimeout(() => {
                this._processBuffer(next);
            }, BULK_TIMEOUT_VALUE);
            */

            next();
        } else {
            //this.bulkTimeout && clearTimeout(this.bulkTimeout);
            this._processBuffer(next);
        }
    }
    _processBuffer(next) {
        const actionsBeingProcessed = this.actionsBuffer.length;
        const bulkArrays = [];
        while (this.actionsBuffer.length > 0) {
            const actionsPartition = this.actionsBuffer.splice(0, Math.ceil(BULK_THRESHOLD / BULK_PARALLEL_BULKS));
            const bulkArray = [];
            actionsPartition.forEach(action => {
                bulkArray.push(action.action);
                bulkArray.push(action.payload);
            });
            bulkArrays.push(bulkArray);
        }

        this.emit('bulk_started', actionsBeingProcessed);

        Promise.all(bulkArrays.map(ba => this._writeToElastic(ba)))
            .then(response => {
                this.emit('bulk_finished', actionsBeingProcessed);
                next();
            })
            .catch(error => {
                console.log(error);
            });
    }
    _writeToElastic(bulkArray) {
        return this.esClient.bulk({
            body: bulkArray
        });
    }
};