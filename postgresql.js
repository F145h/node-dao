let sql_cursor = require('./cursor.js');

function postgresql_connection(c, t) {
    this.connection = c;
    this.table = t;
}

function postgresqlStringFromConditions(conditions, valueNumber) {
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
                        queryStr += " " + colName + " = $" + ++valueNumber + " ";
                        break;
                    case "$ne":
                        queryStr += " " + colName + " != $" + ++valueNumber + " ";
                        break;
                    case "$gt":
                        queryStr += " " + colName + " > $" + ++valueNumber + " ";
                        break;
                    case "$lt":
                        queryStr += " " + colName + " < $" + ++valueNumber + " ";
                        break;
                    case "$gte":
                        queryStr += " " + colName + " >= $" + ++valueNumber + " ";
                        break;
                    case "$lte":
                        queryStr += " " + colName + " <= $" + ++valueNumber + " ";
                        break;
                    default:
                        continue;
                }

                queryValues.push(conditionValue);
                afterFirstCondition = true;
            }
        }
    }

    return { v: queryValues, s: queryStr };
}

function postgresqlStringFromUpdateFields(fields, valueNumber) {
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

            queryStr += colName + " = $" + ++valueNumber + " ";

            queryValues.push(fields[colName]);

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

                        queryStr += " " + vName + " = $" + ++valueNumber + " ";
                        queryValues.push(setFields[vName]);

                        afterFirstField = true;
                    }
                    break;
                case "$inc":
                    let incFields = fields[colName];
                    for (let vName in incFields) {
                        if (afterFirstField)
                            queryStr += ", ";

                        queryStr += " " + vName + " = " + vName + " + $" + ++valueNumber + " ";
                        queryValues.push(incFields[vName]);

                        afterFirstField = true;
                    }
                    break;
                case "$dec":
                    let decFields = fields[colName];
                    for (let vName in decFields) {
                        if (afterFirstField)
                            queryStr += ", ";

                        queryStr += " " + vName + " = " + vName + " - $" + ++valueNumber + " ";
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

postgresql_connection.prototype.update = function (conditions, fields, callback) {

    var valueNumber = 0;

    let ur = postgresqlStringFromUpdateFields(fields, valueNumber);

    valueNumber += ur.v.length;

    let cr = postgresqlStringFromConditions(conditions, valueNumber);

    let sqlCmd = 'UPDATE ' + this.table + ' ' + ur.s + cr.s + ";";

    if (callback !== undefined) {
        this.connection.query(sqlCmd, ur.v.concat(cr.v), callback);
    }
    else {
        return new Promise((resolve, reject) => {
            this.connection.query(sqlCmd, ur.v.concat(cr.v), function (err) {
                if (err) return reject(err);
                resolve();
            });
        });
    }
};

postgresql_connection.prototype.delete = function (conditions, callback) {

    let cr = postgresqlStringFromConditions(conditions, 0);
    let sqlCmd = 'DELETE FROM ' + this.table + ' ' + cr.s + ";";

    if (callback !== undefined) {
        this.connection.query(sqlCmd, cr.v, callback);
    }
    else {
        return new Promise((resolve, reject) => {
            this.connection.query(sqlCmd, cr.v, function (err) {
                if (err) return reject(err);
                resolve();
            });
        });
    }
};

postgresql_connection.prototype.find = function (conditions) {

    let table = this.table;
    let thisConnection = this;

    var cur = new sql_cursor();
    cur.count = function (callback) {

        let cr = postgresqlStringFromConditions(conditions, 0);

        var limitStr = new String();
        if (this.limitValue !== null)
            limitStr += "LIMIT " + this.limitValue + " ";
        if (this.skipValue !== null)
            limitStr += "OFFSET " + this.skipValue + " ";

        let queryStr = "SELECT count(*) FROM " + table + " " + cr.s + limitStr + ";"

        if (callback !== undefined) {
            thisConnection.connection.query(queryStr, cr.v, function (err, rows, fields) {
                if (err)
                    callback(err);
                else {
                    var c = 0;
                    if (rows.rows.length !== 0) {
                        let countResult = rows.rows[0];
                        for (var k in countResult) {
                            c = parseInt(countResult[k]);
                            break;
                        }
                    }
                    callback(null, c);
                }
            });
        }
        else {
            return new Promise((resolve, reject) => {
                thisConnection.connection.query(queryStr, cr.v, function (err, rows, fields) {
                    if (err)
                        reject(err);
                    else {
                        var c = 0;
                        if (rows.rows.length !== 0) {
                            let countResult = rows.rows[0];
                            for (var k in countResult) {
                                c = parseInt(countResult[k]);
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
        let cr = postgresqlStringFromConditions(conditions, 0);

        var orderStr = new String();
        var limitStr = new String();

        var isFirstOrder = true;
        for (let sk in this.sortValue) {
            if (isFirstOrder)
                orderStr += "ORDER BY ";

            let sv = this.sortValue[sk];
            if (sv > 0) {
                if (!isFirstOrder) orderStr += ", ";
                orderStr += sk + " ASC ";
            }
            else if (sv < 0) {
                if (!isFirstOrder) orderStr += ", ";
                orderStr += sk + " DESC ";
            }

            isFirstOrder = false;
        }

        if (this.limitValue !== null)
            limitStr += "LIMIT " + this.limitValue + " ";
        if (this.skipValue !== null)
            limitStr += "OFFSET " + this.skipValue + " ";

        let sqlCmd = "SELECT * FROM " + table + " " + cr.s + orderStr + limitStr + ";";

        if (callback !== undefined) {
            thisConnection.connection.query(sqlCmd, cr.v, function (err, results) {
                if (err) return callback(err);
                callback(null, results.rows);
            });
        }
        else {
            return new Promise((resolve, reject) => {

                thisConnection.connection.query(sqlCmd, cr.v, function (err, results) {
                    if (err) return reject(err);
                    resolve(results.rows);
                });
            });
        }
    };

    return cur;
};

postgresql_connection.prototype.findOne = function (conditions, callback) {

    let cr = postgresqlStringFromConditions(conditions, 0);
    let sqlCmd = "SELECT * FROM " + this.table + " " + cr.s + "LIMIT 1;"

    if (callback !== undefined) {
        return new Promise((resolve, reject) => {
            this.connection.query(sqlCmd, cr.v, function (err, results) {
                if (err) return callback(err);
                callback(null, results.rows.length !== 0 ? results.rows[0] : null);
            });
        });
    }
    else {
        return new Promise((resolve, reject) => {
            this.connection.query(sqlCmd, cr.v, function (err, results) {
                if (err) return reject(err);
                resolve(results.rows.length !== 0 ? results.rows[0] : null);
            });
        });
    }
};

postgresql_connection.prototype.insert = function (fields, callback) {

    var nParam = 0;
    var fieldValues = [];
    var sqlCmd = "";

    sqlCmd += 'INSERT INTO ' + this.table + ' ';
    let ftd = Array.isArray(fields) ? fields[0] : fields;

    var fieldsProcessed = 0;
    sqlCmd += "(";
    for (var f in ftd) {
        ++fieldsProcessed;
        sqlCmd += f;
        sqlCmd += Object.keys(ftd).length !== fieldsProcessed ? ", " : " ";
    }
    sqlCmd += ") VALUES ";

    let genInsert = (flds) => {

        sqlCmd += "(";
        for (var f in flds) {
            sqlCmd += "$" + ++nParam + " ";
            fieldValues.push(flds[f]);
            sqlCmd += fieldValues.length % fieldsProcessed !== 0 ? ", " : " ";
        }
        sqlCmd += ")";
    }

    if (Array.isArray(fields)) {
        for (let i in fields) {
            if (i !== "0")
                sqlCmd += ", ";
            genInsert(fields[i]);
        }
    }
    else
        genInsert(fields);

    sqlCmd += " RETURNING id";

    sqlCmd += ";";  

    if (callback !== undefined) {
        this.connection.query(sqlCmd, fieldValues, function (err, res) {
            if (err) return callback(err);

            if ("id" in res.rows[0]) {
                if (res.rows.length === 1) {
                    callback(null, res.rows[0].id);
                }
                else {
                    var ids = [];
                    for (var o in res.rows) {
                        ids.push(res.rows[o].id);
                    }
                    callback(null, (ids));
                }
            }
        });
    }
    else {
        return new Promise((resolve, reject) => {
            this.connection.query(sqlCmd, fieldValues, function (err, res) {
                if (err) return reject(err);
                if ("id" in res.rows[0]) {
                    if (res.rows.length === 1) {
                        resolve(res.rows[0].id);
                    }
                    else {
                        var ids = [];
                        for (var o in res.rows) {
                            ids.push(res.rows[o].id);
                        }
                        resolve(ids);
                    } 
                }
            });
        });
    }
};

postgresql_connection.prototype.createTable = function (columns, callback) {

    var queryValues = [];
    var queryStr = "DROP SEQUENCE IF EXISTS " + this.table + "_ids CASCADE; CREATE SEQUENCE " + this.table + "_ids; CREATE TABLE " + this.table + " (";
    var firstColumn = true;
    for (let k in columns) {
        if (!firstColumn) {
            queryStr += ", ";
        }
        queryStr += k + " " + columns[k];
        firstColumn = false;
    }
    queryStr += ");"


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

postgresql_connection.prototype.dropTable = function (callback) {

    var queryValues = [];
    let queryStr = "DROP SEQUENCE IF EXISTS " + this.table + "_ids CASCADE; DROP TABLE " + this.table + " ;";

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

module.exports = postgresql_connection;
