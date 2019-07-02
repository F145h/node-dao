let sql_cursor = require('./cursor.js');

function sqlite_connection(c, t) {
    this.connection = c;
    this.table = t;
    this.columns = null;
}

function sqliteStringFromConditions(conditions) {
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
                        queryStr += " " + colName + " = ? ";
                        break;
                    case "$ne":
                        queryStr += " " + colName + " != ? ";
                        break;
                    case "$gt":
                        queryStr += " " + colName + " > ? ";
                        break;
                    case "$lt":
                        queryStr += " " + colName + " < ? ";
                        break;
                    case "$gte":
                        queryStr += " " + colName + " >= ? ";
                        break;
                    case "$lte":
                        queryStr += " " + colName + " <= ? ";
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

function sqliteSelectStringFromConditions(conditions) {
    var selectValues = {};
    var valuesArray = [];
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
                        queryStr += " " + colName + " = :" + colName + "val ";
                        break;
                    case "$ne":
                        queryStr += " " + colName + " != :" + colName + "val ";
                        break;
                    case "$gt":
                        queryStr += " " + colName + " > :" + colName + "val ";
                        break;
                    case "$lt":
                        queryStr += " " + colName + " < :" + colName + "val ";
                        break;
                    case "$gte":
                        queryStr += " " + colName + " >= :" + colName + "val ";
                        break;
                    case "$lte":
                        queryStr += " " + colName + " <= :" + colName + "val ";
                        break;
                    default:
                        continue;
                }

                selectValues[":" + colName + "val"] = conditionValue;
                valuesArray.push(conditionValue);
                afterFirstCondition = true;
            }
        }
    }

    return { v: selectValues, a: valuesArray, s: queryStr };
}

function sqliteStringFromUpdateFields(fields) {
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

            queryStr += colName + " = ? ";

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

                        queryStr += " " + vName + " = ? ";
                        queryValues.push(setFields[vName]);

                        afterFirstField = true;
                    }
                    break;
                case "$inc":
                    let incFields = fields[colName];
                    for (let vName in incFields) {
                        if (afterFirstField)
                            queryStr += ", ";

                        queryStr += " " + vName + " = " + vName + " + ? ";
                        queryValues.push(incFields[vName]);

                        afterFirstField = true;
                    }
                    break;
                case "$dec":
                    let decFields = fields[colName];
                    for (let vName in decFields) {
                        if (afterFirstField)
                            queryStr += ", ";

                        queryStr += " " + vName + " = " + vName + " - ? ";
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

sqlite_connection.prototype.getTableColumns = function()
{
    if (this.columns !== null)
        return this.columns;

    let r = this.connection.exec("PRAGMA table_info("+this.table+");")
    var columns = [];
    for (let rowNumber in r[0].values)
    {
        let rowInfo = r[0].values[rowNumber];
        columns.push(rowInfo[1]);
    }
    this.columns = columns;
    return columns;
}

sqlite_connection.prototype.update = function (conditions, fields, callback) {

    let ur = sqliteStringFromUpdateFields(fields);
    let cr = sqliteStringFromConditions(conditions);

    let sqlCmd = 'UPDATE ' + this.table + ' ' + ur.s + cr.s + ";"

    if (callback !== undefined) {
        var r = this.connection.run(sqlCmd, ur.v.concat(cr.v));
        callback(null);
    }
    else {
        return new Promise((resolve, reject) => {
            var r = this.connection.run(sqlCmd, ur.v.concat(cr.v));
            resolve(r);
        });
    }
};

sqlite_connection.prototype.delete = function (conditions, callback) {
    let cr = sqliteStringFromConditions(conditions);
    let sqlCmd = 'DELETE FROM ' + this.table + ' ' + cr.s + ";"

    if (callback !== undefined) {
        var r = this.connection.run(sqlCmd, cr.v);
        callback(null, r);
    }
    else {
        return new Promise((resolve, reject) => {
            var r = this.connection.run(sqlCmd, cr.v);
            resolve(r);
        });
    }
};

sqlite_connection.prototype.find = function (conditions) {

    let table = this.table;

    let thisConnection = this;

    var cur = new sql_cursor();

    var cur = new sql_cursor();
    cur.count = function (callback) {
        let cr = sqliteSelectStringFromConditions(conditions);

        var limitStr = new String();
        if (this.limitValue !== null)
            limitStr += "LIMIT " + this.limitValue + " ";
        if (this.skipValue !== null)
            limitStr += "OFFSET " + this.skipValue + " ";

        let sqlCmd = "SELECT COUNT(*) FROM " + table + " " + cr.s + limitStr + ";"

        if (callback !== undefined) {
            var stmt = thisConnection.connection.prepare(sqlCmd);
            var r = [];
            stmt.bind(cr.a);
            while (stmt.step()) r.push(stmt.get());
            stmt.free();
            callback(r.length !== 0 ? r[0][0] : 0);
        }
        else {
            return new Promise((resolve, reject) => {
                var stmt = thisConnection.connection.prepare(sqlCmd);
                var r = [];
                stmt.bind(cr.a);
                while (stmt.step()) r.push(stmt.get());
                stmt.free();
                resolve(r.length !== 0 ? r[0][0] : 0);
            });
        }
    };

    cur.toArray = function (callback) {
        let cr = sqliteSelectStringFromConditions(conditions);

        var orderStr = new String();
        var limitStr = new String();

        let columns = thisConnection.getTableColumns();

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

        let sqlCmd = "SELECT * FROM " + table + " ";

        if (callback !== undefined) {
            var stmt = thisConnection.connection.prepare(sqlCmd + cr.s + orderStr + limitStr + ";");
            var r = [];
            stmt.bind(cr.a);
            while (stmt.step()) {
                rowValues = stmt.get();
                rowObj = {};
                for (let ci in columns) {
                    rowObj[columns[ci]] = rowValues[ci];
                }
                r.push(rowObj);
            }
            stmt.free();
            callback(null, r);
        }
        else {
            return new Promise((resolve, reject) => {

                var stmt = thisConnection.connection.prepare(sqlCmd + cr.s + orderStr + limitStr + ";");
                var r = [];
                stmt.bind(cr.a);
                while (stmt.step()) {
                    rowValues = stmt.get();
                    rowObj = {};
                    for (let ci in columns) { 
                        rowObj[columns[ci]] = rowValues[ci];
                    }
                    r.push(rowObj);
                }
                stmt.free();
                resolve(r);
            });
        }
    };

    return cur;


};

sqlite_connection.prototype.findOne = function (conditions, callback) {
    
    let cr = sqliteSelectStringFromConditions(conditions);
    let sqlCmd = "SELECT * FROM " + this.table + " " + cr.s + " LIMIT 1;";

    if (callback !== undefined) {
        var stmt = this.connection.prepare(sqlCmd);
        var r = stmt.getAsObject(cr.v);
        stmt.free();
        callback(null, Object.keys(r).length !== 0 ? r : null);
    }
    else {
        return new Promise((resolve, reject) => {
            var stmt = this.connection.prepare(sqlCmd);
            var r = stmt.getAsObject(cr.v);
            stmt.free();
            resolve(Object.keys(r).length !== 0 ? r : null);
        });
    }
};

sqlite_connection.prototype.insert = function (rows, callback) {

    let table = this.table;
    let connection = this.connection;

    function insertRow(fields, callback) {
        var fieldValues = [];
        var sqlCmd = 'INSERT INTO ' + table + ' ';
        if (fields !== null) {
            var fieldsProcessed = 0;
            sqlCmd += "(";
            for (var f in fields) {
                ++fieldsProcessed;
                sqlCmd += f;
                sqlCmd += Object.keys(fields).length !== fieldsProcessed ? ", " : " ";
                fieldValues[":" + f] = fields[f];
            }
            sqlCmd += ") VALUES (";

            for (var f in fields) {
                sqlCmd += "?";
                fieldValues.push(fields[f]);
                sqlCmd += fieldValues.length !== fieldsProcessed ? ", " : " ";
            }
            sqlCmd += ")";

            connection.run(sqlCmd, fieldValues);
            var r = connection.exec("select last_insert_rowid();");
            callback(null, r.length !== 0 ? r[0].values[0][0] : -1);
        }
        else
            callback(null, -1);
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

sqlite_connection.prototype.createTable = function (columns, callback) {
    var queryValues = [];
    var queryStr = "CREATE TABLE " + this.table + " (";
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
        this.connection.run(queryStr, queryValues);
        callback(null);
    }
    else {
        return new Promise((resolve, reject) => {
            this.connection.run(queryStr, queryValues);
            resolve();
        });
    }
};

sqlite_connection.prototype.dropTable = function (name, callback) {
    var queryValues = [];
    let queryStr = "DROP TABLE " + this.table + " ;";
    if (callback !== undefined) {
        this.connection.run(queryStr, queryValues);
    }
    else {
        return new Promise((resolve, reject) => {
            this.connection.run(queryStr, queryValues);
            resolve();
        });

    }
};

module.exports = sqlite_connection;
