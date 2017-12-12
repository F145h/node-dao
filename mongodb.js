let sql_cursor = require('./cursor.js');

function mongodb_connection(c, t) {
    this.connection = c;
    this.table = t;
};

mongodb_connection.prototype.update = function (fields, condition) {
    return new Promise((resolve, reject) => {
        this.connection.collection(this.table).updateMany(condition, fields, function(err, result) {
            if (err) return reject(err);
            resolve(result);
        });
    });
};

mongodb_connection.prototype.updateOne = function (fields, condition) {
    return new Promise((resolve, reject) => {
        this.connection.collection(this.table).updateOne(condition, fields, function(err, result) {
            if (err) return reject(err);
            resolve(result);
        });
    });
};

mongodb_connection.prototype.delete = function (condition) {
    return new Promise((resolve, reject) => {
        this.connection.collection(this.table).deleteMany(condition, function(err, result) {
           if (err) return reject(err);
            resolve(result);
        });
    });
};

mongodb_connection.prototype.deleteOne = function (condition) {
    return new Promise((resolve, reject) => {
        this.connection.collection(this.table).deleteOne(condition, function(err, result) {
            if (err) return reject(err);
            resolve(result);
        });
    });
};

mongodb_connection.prototype.find = function (condition) {
    var req = this.connection.collection(this.table).find(condition);
    return req;
};

mongodb_connection.prototype.findOne = function (condition) {
    return new Promise((resolve, reject) => {
        this.connection.collection(this.table).findOne(condition, function(err, result) {
            if (err) return reject(err);
            resolve(result);
        });
    });
};

mongodb_connection.prototype.insert = function (objects) {
    if (Array.isArray(objects)) {
        return this.insertMany(objects);
    }
    else {
        return this.insertOne(objects);
    }
}

mongodb_connection.prototype.insertOne = function (fields) {
    return new Promise((resolve, reject) => {
        this.connection.collection(this.table).insertOne(fields, function(err, result) {
            if (err) return reject(err);
            resolve(result);
        });
    });
};

mongodb_connection.prototype.insertMany = function (fields) {
    return new Promise((resolve, reject) => {
        this.connection.collection(this.table).insertMany(fields, function(err, result) {
            if (err) return reject(err);
            resolve(result);
        });
    });
};

mongodb_connection.prototype.createTable - function(){
    return new Promise((resolve, reject) => {
        this.connection.createCollection(this.table, function(err, result) {
            if (err) return reject(err);
            resolve(result);
        });
    });
};

mongodb_connection.prototype.dropTable - function(name){
    return new Promise((resolve, reject) => {
        this.connection.collection(this.table).drop(function(err, result) {
            if (err) return reject(err);
            resolve(result);
        });
    });
};

module.exports = mongodb_connection;
