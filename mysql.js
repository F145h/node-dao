let sql_cursor = require('./cursor.js');

function mysql_connection(c, t) {
    this.connection = c;
    this.table = t;
}

function mysqlStringFromConditions(conditions) {
    var queryValues = [];
    var queryStr = new String();

    var afterFirstCondition = false;
    if (Object.keys(conditions).length !== 0) {
        queryStr += "WHERE ";
        for (let colName in conditions) {
            if (afterFirstCondition)
                queryStr += "AND ";

            let colConditions = conditions[colName];
            for (let conditionName in colConditions) {
                let conditionValue = colConditions[conditionName];
                switch (conditionName) {
                    case "$eq":
                        queryStr += " ?? = ";
                        break;
                    case "$ne":
                        queryStr += " ?? != ";
                        break;
                    case "$gt":
                        queryStr += " ?? > ";
                        break;
                    case "$lt":
                        queryStr += " ?? < ";
                        break;
                    case "$gte":
                        queryStr += " ?? >= ";
                        break;
                    case "$lte":
                        queryStr += " ?? <= ";
                        break;
                    default:
                        continue;
                }

                queryStr += "? ";
                queryValues.push(colName);
                queryValues.push(conditionValue);
                afterFirstCondition = true;
            }
        }
    }

    return { v: queryValues, s: queryStr };
}

function mysqlStringFromUpdateFields(fields) {
    var queryValues = [];
    var queryStr = new String();
    var afterFirstField = false;

    if (Object.keys(fields).length === 0) {
        return {v:[], s:""};
    }

    if (!("$set" in fields || "$inc" in fields || "$dec" in fields))
    {
        queryStr += "SET ";
        for (let colName in fields) {
            if (afterFirstField)
                queryStr += ", ";

            queryStr += "?? = ? ";

            queryValues.push(colName);
            queryValues.push(conditionValue);

            afterFirstField = true;
        }
    }
    else {
        queryStr += "SET ";
        for (let colName in fields) {
            switch(colName)
            {
                case "$set":
                    let setFields = fields[colName];
                    for (let vName in setFields) {
                        if (afterFirstField)
                            queryStr += ", ";

                        queryStr += "?? = ? ";
                        queryValues.push(vName);
                        queryValues.push(setFields[vName]);

                        afterFirstField = true;
                    }
                    break;
                case "$inc":
                    let incFields = fields[colName];
                    for (let vName in incFields) {
                        if (afterFirstField)
                            queryStr += ", ";

                        queryStr += "?? = ?? + ? ";
                        queryValues.push(vName);
                        queryValues.push(vName);
                        queryValues.push(incFields[vName]);

                        afterFirstField = true;
                    }
                    break;
                case "$dec":
                    let decFields = fields[colName];
                    for (let vName in decFields) {
                        if (afterFirstField)
                            queryStr += ", ";

                        queryStr += "?? = ?? - ? ";
                        queryValues.push(vName);
                        queryValues.push(vName);
                        queryValues.push(decFields[vName]);

                        afterFirstField = true;
                    }
                    break;
                default:
                    continue;
            }
        }
    }

    return { v: queryValues, s: queryStr };
}

mysql_connection.prototype.update = function (conditions, fields, callback) {

    let ur = mysqlStringFromUpdateFields(fields);
    let cr = mysqlStringFromConditions(conditions);

    var sqlCmd = "UPDATE ?? " + ur.s + cr.s + ";";

    if (callback !== undefined) {
        this.connection.query(sqlCmd, [this.table].concat(ur.v).concat(cr.v), callback);
    }
    else {
        return new Promise((resolve, reject) => {
            this.connection.query(sqlCmd, [this.table].concat(ur.v).concat(cr.v), function (err) {
                if (err) return reject(err);
                resolve();
            });
        });
    }
};

mysql_connection.prototype.find = function(conditions) {

    let thisConnection = this;

    var queryValues = [this.table];

    var cur = new sql_cursor();
    cur.count = function (callback) {

        let r = mysqlStringFromConditions(conditions);
        queryValues = queryValues.concat(r.v);

        var limitStr = new String();
        if (this.limitValue !== null)
            limitStr += "LIMIT " + this.limitValue + " ";
        if (this.skipValue !== null)
            limitStr += "OFFSET " + this.skipValue + " ";

        let queryStr = "SELECT count(*) FROM ?? " + r.s + limitStr + ";";

        if (callback !== undefined) {
            thisConnection.connection.query(queryStr, queryValues, function (err, rows, fields) {
                if (err)
                    callback(err);
                else {
                    var c = 0;
                    if (Array.isArray(rows) && rows.length !== 0) {
                        let countResult = rows[0];
                        for (var k in countResult) {
                            c = countResult[k];
                            break;
                        }
                    }
                    callback(null, c);
                }
            });
        }
        else {
            return new Promise((resolve, reject) => {
                thisConnection.connection.query(queryStr, queryValues, function (err, rows, fields) {
                    if (err)
                        reject(err);
                    else {
                        var c = 0;
                        if (Array.isArray(rows) && rows.length !== 0) {
                            let countResult = rows[0];
                            for (var k in countResult) {
                                c = countResult[k];
                                break;
                            }
                        }
                        resolve(c);
                    }
                });
            });
        }
    };

    cur.toArray = function (callback) {

        let r = mysqlStringFromConditions(conditions);
        queryValues = queryValues.concat(r.v);

        var orderStr = new String();
        var limitStr = new String();

        var isFirstOrder = true;
        for (let sk in this.sortValue) {
            if (isFirstOrder)
                orderStr += "ORDER BY ";

            let sv = this.sortValue[sk];
            if (sv > 0) {
                if (!isFirstOrder) orderStr += ", ";
                orderStr += "?? ASC ";
                queryValues.push(sk);
            }
            else if (sv < 0) {
                if (!isFirstOrder) orderStr += ", ";
                orderStr += "?? DESC ";
                queryValues.push(sk);
            }

            isFirstOrder = false;
        }

        if (this.limitValue !== null)
            limitStr += "LIMIT " + this.limitValue + " ";
        if (this.skipValue !== null)
            limitStr += "OFFSET " + this.skipValue + " ";

        let queryStr = "SELECT * FROM ?? " + r.s + orderStr + limitStr + ";"

        if (callback !== undefined) {
            thisConnection.connection.query(queryStr, queryValues, callback);
        }
        else {
            return new Promise((resolve, reject) => {
                thisConnection.connection.query(queryStr, queryValues, function (err, rows, fields) {
                    if (err)
                        reject(err);
                    else
                        resolve(rows);
                });
            });
        }
    };

    return cur;
};

mysql_connection.prototype.findOne = function (conditions, callback) {

    let r = mysqlStringFromConditions(conditions);

    var queryValues = [this.table].concat(r.v);
    var queryStr = "SELECT * FROM ?? " + r.s + "LIMIT 1;";

    if (callback !== undefined) {
        this.connection.query(queryStr, queryValues, function (err, rows, fields) {
            if (err) return callback(err);
            callback(null, rows.length !== 0 ? rows[0] : null);
        });
    }
    else {
        return new Promise((resolve, reject) => {
            this.connection.query(queryStr, queryValues, function (err, rows, fields) {
                if (err) return reject(err);
                resolve(rows.length !== 0 ? rows[0] : null);
            });
        });
    }
};

mysql_connection.prototype.delete = function (conditions, callback) {

    let r = mysqlStringFromConditions(conditions);

    var queryStr = "DELETE FROM ?? " + r.s + ";";
    var queryValues = [this.table].concat(r.v);

    if (callback !== undefined) {
        this.connection.query(queryStr, queryValues, callback);
    }
    else {
        return new Promise((resolve, reject) => {
            this.connection.query(queryStr, queryValues, function (err) {
                if (err) return reject(err);
                resolve();
            });
        });
    }
};

mysql_connection.prototype.insert = function (rows, callback) {

    let table = this.table;
    let connection = this.connection;

    function insertRow(rows, callback) {
        var queryValues = [table, rows];

        if (callback !== undefined) {
            if (rows.length === 0) {
                return callback(-1);
            }

            connection.query('INSERT INTO ?? SET ?', queryValues, function (err, res) {
                if (err) return callback(err);
                callback(null, res.insertId);
            });
        }
    }

    function sendRows(rows, callback) {
        let nRows = rows.length;
        var ids = [];
        var currentRow = 0;

        function sendNextRow(rowNumber, callback) {
            insertRow(rows[rowNumber], (err, res) => {
                if (err)
                    return callback(err);

                ++rowNumber;
                ids.push(res);
                if (nRows !== rowNumber)
                    return sendNextRow(rowNumber, callback);
                else
                    callback(null, ids);
            });
        }

        sendNextRow(0, callback);
    };

    if (callback !== undefined) {
        if (Array.isArray(rows)) {
            if (rows.length === 0) {
                return callback(-1);
            }

            sendRows(rows, function (err, res) {
                if (err) return callback(err);
                callback(null, res);
            });
        }
        else
            return insertRow(rows, callback);
    }
    else {
        return new Promise((resolve, reject) => {
            if (Array.isArray(rows)) {
                if (rows.length === 0) {
                    return resolve(-1);
                }

                sendRows(rows, function (err, res) {
                    if (err) return reject(err);
                    resolve(res);
                });
            }
            else {
                insertRow(rows, function (err, res) {
                    if (err) return reject(err);
                    resolve(res.insertId);
                });
            }
        });
    }

};

mysql_connection.prototype.createTable = function (columns, callback) {

    var queryValues = [this.table];
    var queryStr = "CREATE TABLE ?? (";
    var firstColumn = true;
    for (let k in columns) {
        if (!firstColumn) {
            queryStr += ", ";
        }
        queryStr += k + " " + columns[k];
        firstColumn = false;
    }
    queryStr += ");";

    if (callback !== undefined) {
        this.connection.query(queryStr, queryValues, callback);
    }
    else {
        return new Promise((resolve, reject) => {
            this.connection.query(queryStr, queryValues, function (err) {
                if (err) return reject(err);
                resolve();
            });
        });
    }
};

mysql_connection.prototype.dropTable = function (callback){
    var queryValues = [this.table];
    var queryStr = "DROP TABLE ??;";

    if (callback !== undefined) {
        this.connection.query(queryStr, queryValues, callback);
    }
    else {
        return new Promise((resolve, reject) => {
            this.connection.query(queryStr, queryValues, function (err) {
                if (err) return reject(err);
                resolve();
            });
        });
    }
};


module.exports = mysql_connection;
