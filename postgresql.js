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
                queryStr += "AND ";

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
                            queryStr += "AND ";

                        queryStr += " " + vName + " = $" + ++valueNumber + " ";
                        queryValues.push(setFields[vName]);
                    }
                    break;
                case "$inc":
                    let incFields = fields[colName];
                    for (let vName in incFields) {
                        if (afterFirstField)
                            queryStr += "AND ";

                        queryStr += " " + vName + " = " + vName + " + $" + ++valueNumber + " ";
                        queryValues.push(incFields[vName]);
                    }
                    break;
                case "$dec":
                    let decFields = fields[colName];
                    for (let vName in decFields) {
                        if (afterFirstField)
                            queryStr += "AND ";

                        queryStr += " " + vName + " = " + vName + " + $" + ++valueNumber + " ";
                        queryValues.push(decFields[vName]);
                    }
                    break;
                default:
                    continue;
            }
        }
    }

    return { v: queryValues, s: queryStr };
}

postgresql_connection.prototype.update = function (fields, conditions, callback) {
    return new Promise((resolve, reject) => {
        var sqlCmd = 'UPDATE ' + this.table + ' ';
        var valueNumber = 0;

        let ur = postgresqlStringFromUpdateFields(fields, valueNumber);

        valueNumber += ur.v.length;

        let cr = postgresqlStringFromConditions(conditions, valueNumber);

        this.connection.query(sqlCmd + ur.s + cr.s + ";", ur.v.concat(cr.v), function (err) {
            if (err) return reject(err);
            resolve();
        });
    });
};

postgresql_connection.prototype.delete = function (conditions, callback) {
    return new Promise((resolve, reject) => {
        var sqlCmd = 'DELETE FROM ' + this.table + ' ';
        let cr = postgresqlStringFromConditions(conditions, 0);

        this.connection.query(sqlCmd + cr.s + ";", cr.v, function (err) {
            if (err) return reject(err);
            resolve();
        });
    });
};

postgresql_connection.prototype.find = function (conditions) {

    var sqlCmd = "SELECT * FROM " + this.table + " ";
    let cr = postgresqlStringFromConditions(conditions, 0);
console.log(conditions, cr);
    let thisConnection = this;

    var cur = new sql_cursor();
    cur.toArray = function (callback) {
        return new Promise((resolve, reject) => {
            var orderStr = new String();
            var limitStr = new String();

            var isFirstOrder = true;
            for (let sk in this.sort) {
                if (isFirstOrder)
                    orderStr += "ORDER BY ";

                let sv = this.sort[sk];
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

            if (this.count !== null)
                limitStr += "LIMIT " + this.count + " ";
            if (this.offset !== null)
                limitStr += "OFFSET " + this.offset + " ";

            thisConnection.connection.query(sqlCmd + cr.s + orderStr + limitStr + ";", cr.v, function (err, results) {
                if (err) return reject(err);
                resolve(results.rows);
            });
        });
    };

    return cur;
};

postgresql_connection.prototype.findOne = function (conditions, callback) {
    return new Promise((resolve, reject) => {
        var sqlCmd = "SELECT * FROM " + this.table + " ";
        let cr = postgresqlStringFromConditions(conditions, 0);

        this.connection.query(sqlCmd + cr.s + "LIMIT 1;", cr.v, function (err, results) {
            if (err) return reject(err);
            resolve(results.rows.length > 1 ? result.rows[0] : null);
        });
    });
};

postgresql_connection.prototype.insert = function (fields, callback) {
    return new Promise((resolve, reject) => {
        var fieldValues = [];
        var sqlCmd = 'INSERT INTO ' + this.table + ' ';
        if (fields !== null) {
            var fieldsProcessed = 0;
            sqlCmd += "(";
            for (var f in fields) {
                ++fieldsProcessed;
                sqlCmd += f;
                sqlCmd += Object.keys(fields).length !== fieldsProcessed ? ", " : " ";
            }
            sqlCmd += ") VALUES (";

            var nParam = 0;
            for (var f in fields) {
                sqlCmd += "$" + ++nParam + " ";
                fieldValues.push(fields[f]);
                sqlCmd += fieldValues.length !== fieldsProcessed ? ", " : " ";
            }
            sqlCmd += ")";
        }
        this.connection.query(sqlCmd + ";", fieldValues, function (err) {
            if (err) return reject(err);
            resolve();
        });
    });
};

postgresql_connection.prototype.createTable - function(name, columns){
    return new Promise((resolve, reject) => {
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
        queryStr += ")'"

        this.connection.query(queryStr + r.s + ";", queryValues, function (err) {
            if (err) return reject(err);
            resolve();
        });
    });
};

postgresql_connection.prototype.dropTable - function(name){
    return new Promise((resolve, reject) => {
        var queryValues = [this.table];
        var queryStr = "DROP TABLE ?? ";
        this.connection.query(queryStr + ";", queryValues, function (err) {
            if (err) return reject(err);
            resolve();
        });
    });
};

module.exports = postgresql_connection;