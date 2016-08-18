'use strict';

const stream = require('stream');

module.exports = class ArrayStream extends stream.Readable {
    constructor(array) {
        super();
        this.array = array;
    }
    _read() {
        const element = this.array.pop();
        if (element) {
            this.push(element);
        } else {
            this.push(null);
        }
    }
};