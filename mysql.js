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

                        queryStr += "?? = ? ";
                        queryValues.push(vName);
                        queryValues.push(setFields[vName]);
                    }
                    break;
                case "$inc":
                    let incFields = fields[colName];
                    for (let vName in incFields) {
                        if (afterFirstField)
                            queryStr += "AND ";

                        queryStr += "?? = ?? + ? ";
                        queryValues.push(vName);
                        queryValues.push(vName);
                        queryValues.push(incFields[vName]);
                    }
                    break;
                case "$dec":
                    let decFields = fields[colName];
                    for (let vName in decFields) {
                        if (afterFirstField)
                            queryStr += "AND ";

                        queryStr += "?? = ?? - ? ";
                        queryValues.push(vName);
                        queryValues.push(vName);
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

mysql_connection.prototype.update = function(fields, condition, callback) {

    return new Promise((resolve, reject) => {
        var sqlCmd = "UPDATE ?? ";

        let ur = mysqlStringFromUpdateFields(fields);
        let cr = mysqlStringFromConditions(condition);

        this.connection.query(sqlCmd + ur.s + cr.s + ";", [this.table].concat(ur.v).concat(cr.v), function (err) {
            if (err) return reject(err);
            resolve();
        });
    });
};

mysql_connection.prototype.find = function(conditions) {

    let thisConnection = this;

    var queryValues = [this.table];

    var cur = new sql_cursor();
    cur.count = function (callback) {
        return new Promise((resolve, reject) => {
            var queryStr = "SELECT count(*) FROM ?? ";

            let r = mysqlStringFromConditions(conditions);
            queryValues = queryValues.concat(r.v);

            var limitStr = new String();
            if (this.limitValue !== null)
                limitStr += "LIMIT " + this.limitValue + " ";
            if (this.skipValue !== null)
                limitStr += "OFFSET " + this.skipValue + " ";

            thisConnection.connection.query(queryStr + r.s + limitStr +";", queryValues, function (err, rows, fields) {
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
    };

    cur.toArray = function (callback) {
        return new Promise((resolve, reject) => {

            var queryStr = "SELECT * FROM ?? ";

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

            thisConnection.connection.query(queryStr + r.s + orderStr + limitStr + ";", queryValues, function (err, rows, fields) {
                if (err)
                    reject(err);
                else
                    resolve(rows);
            });
        });
    };

    return cur;
};

mysql_connection.prototype.findOne = function (conditions, callback) {

    return new Promise((resolve, reject) => {
        var queryValues = [this.table];
        var queryStr = "SELECT * FROM ?? ";
        let r = mysqlStringFromConditions(conditions);

        this.connection.query(queryStr + r.s + "LIMIT 1;", queryValues.concat(r.v), function (err, rows, fields) {
            if (err) return reject(err);
            resolve(rows.length > 0 ? rows[0] : null);
        });
    });
};

mysql_connection.prototype.delete = function (conditions, callback) {

    return new Promise((resolve, reject) => {
        var queryValues = [this.table];
        var queryStr = "DELETE FROM ?? ";
        let r = mysqlStringFromConditions(conditions);

        this.connection.query(queryStr + r.s + ";", queryValues.concat(r.v), function (err) {
            if (err) return reject(err);
            resolve();
        });
    });
};

mysql_connection.prototype.insert = function (rows, callback) {

    return new Promise((resolve, reject) => {
        if (rows.length === 0) {
            return callback(null);
        }

        var queryValues = [this.table];
        var queryStr = "INSERT INTO ?? (";
        let firstRow = rows[0];
        let firstColumn = true;
        for (let k in firstRow) {
            if (firstColumn) {
                firstColumn = false;
                queryStr += "?? ";
            }
            else {
                queryStr += ", ?? ";
            }
            queryValues.push(k);
        }
        queryStr += ") VALUES ? ;";

        queryValues.push(rows);

        this.connection.query("INSERT INTO ?? SET ?;", queryValues, function (err) {
            if (err) return reject(err);
            resolve();
        });
    });
};

mysql_connection.prototype.createTable - function(columns){
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

mysql_connection.prototype.dropTable - function(name){
    return new Promise((resolve, reject) => {
        var queryValues = [this.table];
        var queryStr = "DROP TABLE ?? ";
        this.connection.query(queryStr + ";", queryValues, function (err) {
            if (err) return reject(err);
            resolve();
        });
    });
};


module.exports = mysql_connection;
