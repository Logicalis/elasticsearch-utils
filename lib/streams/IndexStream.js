const Transform = require('stream').Transform;

module.exports = class IndexStream extends Transform {
    constructor(options) {
        super(options);
        this.index = options.index;
        this.type = options.type;
        this.idGetter = options.idGetter;
    }

    _transform(chunk, encoding, callback) {
        const docs = JSON.parse(chunk.toString());

        const actionObjects = docs.map(doc => {
            return {
                action: { index:  { _index: this.index, _type: this.type, _id: this.idGetter && this.idGetter(doc) } },
                payload: doc
            };
        });

        callback(null, JSON.stringify(actionObjects));
    }
};