var riakpbc = require('riakpbc');
var stream = require('stream');
var util = require('util');
    
function reverseString(sin) {
    var out = '';
    for (var idx = 0, l = sin.length; idx < l; idx++) { 
        out += String.fromCharCode(255 - sin[idx].charCodeAt())
    }
    return out;
}

function GetValueTransform(riak, opts) {
    if (typeof opts === 'undefined') {
        opts = {};
    }
    if (!opts.highWaterMark) opts.highWaterMark = 1;
    if (!opts.hasOwnProperty('allowHalfOpen')) opts.allowHalfOpen = false;
    if (!opts.hasOwnProperty('objectMode')) opts.objectMode = true;
    this.riak = riak;
    this.bucket = opts.bucket || 'default_levelup';
    if (opts.keys !== false) {
        this.keys = true;
    } else {
        this.keys = false;
    }
    stream.Transform.call(this, opts);
}

util.inherits(GetValueTransform, stream.Transform);

(function () {
    this._transform = function (chunk, encoding, next) {
        this.riak.get({key: chunk, bucket: this.bucket}, function (err, value) {
            if (!value.content) {
                err = "Not found";
            }
            if (!err && value) {
                if (this.keys) {
                    value.content[0].value.vclock = value.vclock;
                    this.push({key: chunk, value: value.content[0].value});
                } else {
                    this.push(value.content.value);
                }
            }
            next();
        }.bind(this));
    };
}).call(GetValueTransform.prototype);

function SplitKeyTransform(opts) {
    if (typeof opts === 'undefined') {
        opts = {};
    }
    if (!opts.highWaterMark) opts.highWaterMark = 1;
    if (!opts.hasOwnProperty('allowHalfOpen')) opts.allowHalfOpen = false;
    if (!opts.hasOwnProperty('objectMode')) opts.objectMode = true;
    stream.Transform.call(this, opts);
}

util.inherits(SplitKeyTransform, stream.Transform);

(function () {
    this._transform = function (chunk, encoding, next) {
        chunk.keys.forEach(function (key) {
            this.push(key);
        }.bind(this));
        next();
    };
}).call(SplitKeyTransform.prototype);

function ReverseKeyTransform(opts) {
    if (typeof opts === 'undefined') {
        opts = {};
    }
    if (!opts.highWaterMark) opts.highWaterMark = 1;
    if (!opts.hasOwnProperty('allowHalfOpen')) opts.allowHalfOpen = false;
    if (!opts.hasOwnProperty('objectMode')) opts.objectMode = true;
    stream.Transform.call(this, opts);
}

util.inherits(ReverseKeyTransform, stream.Transform);

(function () {
    this._transform = function (chunk, encoding, next) {
        this.push(reverseString(chunk));
        next();
    };
}).call(ReverseKeyTransform.prototype);

function LevelUpRiak(riak_conf, options, callback) {
    if (typeof options === 'function') {
        callback = options;
        options = {};
    }
    this.riak_conf = riak_conf;
    this.options = options;

    this.options.keyEncoding = this.options.keyEncoding || 'utf8';
    this.options.valueEncoding = this.options.valueEncoding || 'utf8';

    this.riak = null;

    this.open(callback);
}

(function () {

    var lupr = this;

    this.isRiak = true;

    this.open = function (cb) {
        this.riak = riakpbc.createClient(this.riak_conf);
        if (typeof cb === 'function') {
            this.riak.connect(cb);
        }
    };

    this.close = function (cb) {
        this.riak.disconnect(cb);
    };

    this.put = function (key, value, opts, cb) {
        if (typeof opts === 'function') {
            cb = opts;
            opts = {};
        }
        var reversed_indexes = [];
        if (!Array.isArray(opts.indexes)) {
            opts.indexes = [];
        }
        opts.indexes.forEach(function (index) {
            if (index.key.substr(-4) === '_bin') {
                reversed_indexes.push({
                    key: '__reverse__' + index.key,
                    value: reverseString(index.value),
                });
            } else {
                reversed_indexes.push({
                    key: '__reverse__' + index.key,
                    value: (index.value * -1),
                });
            }
        });
        opts.indexes = opts.indexes.concat(reversed_indexes);
        opts.indexes.push({key: '__reverse_key_bin', value: reverseString(key)});

        var args = {
            bucket: opts.bucket || 'default_levelup',
            key: key,
            w: 'all',
            return_head: true,
            content: {
                value: JSON.stringify(value),
                content_type: 'application/json',
                indexes: opts.indexes,
            }
        };
        if (opts.vclock) {
            args.vclock = opts.vclock;
        }
        this.riak.put(args, function (err, reply) {
            if (opts.withVClock) {
                cb(err, reply.vclock);
            } else {
                cb(err);
            }
        });
    };

    this.get = function (key, opts, cb) {
        if (typeof opts === 'function') {
            cb = opts;
            opts = {};
        }
        var args = {key: key, bucket: opts.bucket || 'default_levelup'};
        this.riak.get(args, function (err, reply) {
            if (!reply.content) {
                cb("Not found");
                return;
            }
            if (reply.content.length > 1) {
                //siblings
                //cull deletes
                var content = [];
                for (var sidx = 0, l = reply.content.length; sidx < l; sidx++) {
                    if (reply.content[sidx].deleted !== true) {
                        content.push(reply.content[sidx]);
                    }
                }
                reply.content = content;
            }
            if (reply.content.length > 1) {
                if (typeof this.handleSiblings === 'function') {
                    this.handleSiblings(reply);
                }
            }
            if (!err && reply) {
                reply.content[0].value.vclock = reply.vclock;
                cb(err, reply.content[0].value);
            } else {
                cb(err);
            }
        }.bind(this));
    };

    this.del = function (key, opts, cb) {
        if (typeof opts === 'function') {
            cb = opts;
            opts = {};
        }
        this.riak.del({key: key, bucket: opts.bucket || 'default_levelup'}, function (err) {
            cb(err);
        });
    };

    this.batch = function (calls, opts, cb) {
        if (arguments.length === 0) {
            return new Batch(this);
        }
    };

    this.isOpen = function () {
        return this.riak.connection.connected;
    };

    this.isClosed = function () {
        //return !this.riak.connection.connected;
        return false;
    };

    this.readStream = function (opts) {
        var nextstream;
        if (typeof opts.values === 'undefined') opts.values = true;
        if (!opts.limit) opts.limit = -1;
        if (opts.reverse) {
            if (!opts.index) {
                opts.index = '__reverse_key_bin';
            } else {
                opts.index = '__reverse__' + opts.index;
            }
            if (opts.index.substr(-4) === '_bin') {
                opts.start = reverseString(opts.start);
                opts.end = reverseString(opts.end);
            } else {
                opts.start *= -1;
                opts.end *= -1;
            }
            var args = {
                index: opts.index,
                pagination_sort: true,
                qtype: 1,
                range_min: opts.end,
                range_max: opts.start,
                bucket: opts.bucket || 'default_levelup'
            }
            if (opts.limit !== -1) args.max_result = opts.limit;
            nextstream = this.riak.getIndex(args);
            nextstream = nextstream.pipe(new SplitKeyTransform());
            //nextstream = nextstream.pipe(new ReverseKeyTransform());
        } else {
            if (!opts.index) {
                opts.index = '$key';
            }
            var args = {
                index: opts.index,
                qtype: 1,
                pagination_sort: true,
                range_min: opts.start,
                range_max: opts.end,
                bucket: opts.bucket || 'default_levelup'
            };
            if (opts.limit !== -1) args.max_result = opts.limit;
            nextstream = this.riak.getIndex(args);
            nextstream = nextstream.pipe(new SplitKeyTransform());
        }
        if (opts.values === true) {
            nextstream = nextstream.pipe(new GetValueTransform(this.riak, {keys: opts.keys, bucket: opts.bucket}));
        }
        return nextstream;
    };

    this.createReadStream = this.readStream;

    this.createKeyStream = function (opts) {
        opts.values = false;
        return this.readStream(opts);
    };

    this.createvalueStream = function (opts) {
        opts.values = true;
        opts.keys = false;
        return this.readStream(opts);
    };
    
    this.writeStream = function (opts) {
    };
    
}).call(LevelUpRiak.prototype);

function Batch(lup) {
    this.lup = lup;
    this.queue = [];
}

(function () {

    this.put = function () {
        return this;
    };

    this.del = function () {
        return this;
    };

    this.write = function () {
        return this;
    };

}).call(Batch.prototype);


module.exports = function (config, options) {
    return new LevelUpRiak(config, options);
};
