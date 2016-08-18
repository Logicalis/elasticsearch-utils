'use strict';

const Transform = require('stream').Transform;

/**
 * Class representing an Elasticsearch Index stream.
 * @extends stream.Readable
 */
class IndexStream extends Transform {
    /**
     * Create a IndexStream.
     * @param {object} options - Stream options.
     * @param {string} options.index - Target index where documents will be written to.
     * @param {string} options.type - Type of documents being written.
     * @param {idGetter} options.idGetter - Document's id getter function.
     */
    constructor(options) {
        super(options);

        this.index = options.index;
        this.type = options.type;
        this.idGetter = options.idGetter;
    }

    _transform(chunk, encoding, callback) {
        let docs = JSON.parse(chunk.toString());

        if (!(docs instanceof Array)) {
            docs = [docs];
        }

        const actionObjects = docs.map(doc => {
            return {
                action: { index:  { _index: this.index, _type: this.type, _id: this.idGetter && this.idGetter(doc) } },
                payload: doc
            };
        });

        callback(null, JSON.stringify(actionObjects));
    }
}

/**
 * This callback is displayed as part of the IndexStream class.
 * This must be a function that returns the desired document's id.
 * @callback idGetter
 * @param {object} doc - Document object.
 * @returns {string} - Document's id.
 */

module.exports = IndexStream;