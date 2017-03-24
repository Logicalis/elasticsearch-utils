'use strict';

const stream = require('stream');

/**
 * Class representing an Elasticsearch Scroll stream.
 * @extends stream.Readable
 */
class ScrollerStream extends stream.Readable {
    /**
     * Create a ScrollerStream.
     * @param {object} options - Stream options.
     * @param {ElasticsearchClient} options.esClient - Elasticsearch client. More info: https://www.elastic.co/guide/en/elasticsearch/client/javascript-api/current/index.html
     * @param {string} options.index - Index to be scrolled.
     * @param {string} options.type - Type of documents to be scrolled.
     * @param {number} options.size - Response size of each scroll iteration.
     */
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
                console.log(`[ScrollerStream] ${this.scrolledDocsLength} docs has been read`);
                if (this.scrolledDocsLength < response.hits.total) {
                    this._readData(response);
                    //this.resume();
                } else {
                    this.push(null);
                }
            });
        } else {
            console.log(`[ScrollerStream] Starting scroller...`);
            this.esClient.search({
                index: this.index,
                type: this.type,
                scroll: '30s',
                size: this.size
                //search_type: 'scan'
            }).then((response) => {
                console.log(`[ScrollerStream] Total of ${response.hits.total} docs will be read`);
                console.log(`[ScrollerStream] ${this.scrolledDocsLength} docs has been read`);
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
}

module.exports = ScrollerStream;