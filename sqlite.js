let sql_cursor = require('./cursor.js');

function sqlite_connection(c, t) {
    this.connection = c;
    this.table = t;
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
                queryStr += "AND ";

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
                            queryStr += "AND ";

                        queryStr += " " + vName + " = ? ";
                        queryValues.push(setFields[vName]);
                    }
                    break;
                case "$inc":
                    let incFields = fields[colName];
                    for (let vName in incFields) {
                        if (afterFirstField)
                            queryStr += "AND ";

                        queryStr += " " + vName + " = " + vName + " + ? ";
                        queryValues.push(incFields[vName]);
                    }
                    break;
                case "$dec":
                    let decFields = fields[colName];
                    for (let vName in decFields) {
                        if (afterFirstField)
                            queryStr += "AND ";

                        queryStr += " " + vName + " = " + vName + " + ? ";
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

sqlite_connection.prototype.update = function (fields, conditions, callback) {
    return new Promise((resolve, reject) => {
        var sqlCmd = 'UPDATE ' + this.table + ' ';

        let ur = sqliteStringFromUpdateFields(fields);
        let cr = sqliteStringFromConditions(conditions);

        var r = this.connection.run(sqlCmd + ur.s + cr.s + ";", ur.v.concat(cr.v));
        resolve(r);
    });
};

sqlite_connection.prototype.delete = function (conditions, callback) {
    return new Promise((resolve, reject) => {
        var sqlCmd = 'DELETE FROM ' + this.table + ' ';
        let cr = sqliteStringFromConditions(conditions);
        var r = this.connection.run(sqlCmd + cr.s + ";", cr.v);
        resolve(r);
    });
};

sqlite_connection.prototype.find = function (conditions) {

    let table = this.table;

    let thisConnection = this;

    var cur = new sql_cursor();

    var cur = new sql_cursor();
    cur.count = function (callback) {
        return new Promise((resolve, reject) => {
            var sqlCmd = "SELECT COUNT(*) FROM " + table + " ";

            let cr = sqliteSelectStringFromConditions(conditions);

            var stmt = thisConnection.connection.prepare(sqlCmd + cr.s + ";");
            var r = [];
            stmt.bind(cr.a);
            while (stmt.step()) r.push(stmt.get());
            resolve(r.length !== 0 ? r[0][0] : 0);
        });
    };

    cur.toArray = function (callback) {
        return new Promise((resolve, reject) => {
            var sqlCmd = "SELECT * FROM " + table + " ";
            let cr = sqliteSelectStringFromConditions(conditions);

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
            var stmt = thisConnection.connection.prepare(sqlCmd + cr.s + orderStr + limitStr + ";");
            var r = [];
            stmt.bind(cr.a);
            while (stmt.step()) r.push(stmt.get());
            resolve(r);
        });
    };

    return cur;


};

sqlite_connection.prototype.findOne = function (conditions, callback) {
    return new Promise((resolve, reject) => {
        var sqlCmd = "SELECT * FROM " + this.table + " ";
        let cr = sqliteSelectStringFromConditions(conditions);

        var stmt = this.connection.prepare(sqlCmd + cr.s + " LIMIT 1;");
        var r = stmt.getAsObject(cr.v);
        resolve(Object.keys(r).length > 0 ? r : null);
    });
};

sqlite_connection.prototype.insert = function (fields, callback) {
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
                fieldValues[":" + f] = fields[f];
            }
            sqlCmd += ") VALUES (";

            for (var f in fields) {
                sqlCmd += "?";
                fieldValues.push(fields[f]);
                sqlCmd += fieldValues.length !== fieldsProcessed ? ", " : " ";
            }
            sqlCmd += ")";
        }
        var r = this.connection.run(sqlCmd, fieldValues);
        resolve(r);
    });
};

sqlite_connection.prototype.createTable - function(columns){
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

        this.connection.run(queryStr + ";", queryValues);
        resolve(r);
    });
};

sqlite_connection.prototype.dropTable - function(name){
    return new Promise((resolve, reject) => {
        var queryValues = [this.table];
        var queryStr = "DROP TABLE ?? ";
        this.connection.run(queryStr + ";", queryValues);
        resolve(r);
    });
};

module.exports = sqlite_connection;
