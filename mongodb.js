let sql_cursor = require('./cursor.js');

function mongodb_connection(c, t) {
    this.connection = c;
    this.table = t;
};

mongodb_connection.prototype.update = function (conditions, fields, callback) {
    if (callback !== undefined) {
        this.connection.collection(this.table).updateMany(conditions, fields, callback);
    }
    else {
        return new Promise((resolve, reject) => {
            this.connection.collection(this.table).updateMany(conditions, fields, function (err, result) {
                if (err) return reject(err);
                resolve(result);
            });
        });
    }
};

mongodb_connection.prototype.updateOne = function (conditions, fields, callback) {
    if (callback !== undefined) {
        this.connection.collection(this.table).updateOne(conditions, fields, callback);
    }
    else {
        return new Promise((resolve, reject) => {
            this.connection.collection(this.table).updateOne(conditions, fields, function (err, result) {
                if (err) return reject(err);
                resolve(result);
            });
        });
    }
};

mongodb_connection.prototype.delete = function (condition, callback) {
    if (callback !== undefined) {
        this.connection.collection(this.table).deleteMany(condition, callback);
    }
    else {
        return new Promise((resolve, reject) => {
            this.connection.collection(this.table).deleteMany(condition, function (err, result) {
                if (err) return reject(err);
                resolve(result);
            });
        });
    }
};

mongodb_connection.prototype.deleteOne = function (condition, callback) {
    if (callback !== undefined) {
        this.connection.collection(this.table).deleteOne(condition, callback);
    }
    else {
        return new Promise((resolve, reject) => {
            this.connection.collection(this.table).deleteOne(condition, function (err, result) {
                if (err) return reject(err);
                resolve(result);
            });
        });
    }
};

mongodb_connection.prototype.find = function (condition) {
    var req = this.connection.collection(this.table).find(condition);
    return req;
};

mongodb_connection.prototype.findOne = function (condition, callback) {
    if (callback !== undefined) {
        this.connection.collection(this.table).findOne(condition, callback);
    }
    else {
        return new Promise((resolve, reject) => {
            this.connection.collection(this.table).findOne(condition, function (err, result) {
                if (err) return reject(err);
                resolve(result);
            });
        });
    }
};

mongodb_connection.prototype.insert = function (objects, callback) {
    if (Array.isArray(objects)) {
        if (callback !== undefined) {
            return this.insertMany(objects, function (err, res) {
                if (err)
                    return callback(err);
                var ids = [];
                for (var o in res.ops) {
                    ids.push(res.ops[o].id);
                }
                return callback(null, ids);
            });
        }
        else {
            return new Promise((resolve, reject) => {
                return this.insertMany(objects, function (err, res) {
                    if (err) return reject(err);
                    var ids = [];
                    for (var o in res.ops) {
                        ids.push(res.ops[o].id);
                    }
                    return resolve(ids);
                });
            });
        }
    }
    else {
        return this.insertOne(objects, callback);
    }
}

mongodb_connection.prototype.insertOne = function (fields, callback) {
    if (callback !== undefined) {
        this.connection.collection(this.table).insertOne(fields, function (err, result) {
            if (err) return callback(err);
            callback(null, result.insertedId.toHexString());
        });
    }
    else {
        return new Promise((resolve, reject) => {
            this.connection.collection(this.table).insertOne(fields, function (err, result) {
                if (err) return reject(err);
                resolve(result.insertedId.toHexString());
            });
        });
    }
};

mongodb_connection.prototype.insertMany = function (fields, callback) {
    if (callback !== undefined) {
        this.connection.collection(this.table).insertMany(fields, callback);
    }
    else {
        return new Promise((resolve, reject) => {
            this.connection.collection(this.table).insertMany(fields, function (err, result) {
                if (err) return reject(err);
                resolve(result);
            });
        });
    }
};

mongodb_connection.prototype.createTable = function (callback) {
    if (callback !== undefined) {
        this.connection.createCollection(this.table, callback);
    }
    else {
        return new Promise((resolve, reject) => {
            this.connection.createCollection(this.table, function (err, result) {
                if (err) return reject(err);
                resolve(result);
            });
        });
    }
};

mongodb_connection.prototype.dropTable = function (name, callback) {
    if (callback !== undefined) {
        this.connection.collection(this.table).drop(callback);
    }
    else {
        return new Promise((resolve, reject) => {
            this.connection.collection(this.table).drop(function (err, result) {
                if (err) return reject(err);
                resolve(result);
            });
        });
    }
};

module.exports = mongodb_connection;
