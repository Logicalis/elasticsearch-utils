/**
 *
 * ScrollerStream - a documents scroller stream for elasticsearch
 * (vhiroki - re91031z)
 *
 */

'use strict';

const stream = require('stream');

module.exports = class ScrollerStream extends stream.Readable {
    constructor(options) {
        super(options);
        this.esClient = options.esClient;
        this.index = options.index;
        this.type = options.type;
        this.size = options.size;
        this.scrollId = null;
        this.scrolledDocsLength = 0;
    }
    _read() {
        if (this.scrollId) {
            this.esClient.scroll({
                scrollId: this.scrollId,
                scroll: '30s'
            }).then(response => {
                if (this.scrolledDocsLength < response.hits.total) {
                    this._readData(response);
                    //this.resume();
                } else {
                    this.push(null);
                }
            });
        } else {
            this.esClient.search({
                index: this.index,
                type: this.type,
                scroll: '30s',
                size: this.size
                //search_type: 'scan'
            }).then((response) => {
                if (this.scrolledDocsLength < response.hits.total) {
                    this.scrollId = response._scroll_id;
                    this._readData(response);
                    //this.resume();
                } else {
                    this.push(null);
                }
            });
        }

        //this.pause();
    }
    _readData(response) {
        const docs = response.hits.hits.map(hit => hit._source);
        this.scrolledDocsLength += docs.length;

        this.push(JSON.stringify(docs));
    }
};