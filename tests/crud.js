let dao = require('../index.js');

let mysql = require("mysql");
let pg = require("pg");
let mongodb = require("mongodb");
let sqlite = require("sql.js");

let Ok = "[OK]";
let Failed = "[FAILED]";


function check(n, c, td) {
    return new Promise(async function(resolve, reject){
        console.log("--------------------------------------\nDatabase type:", n, "\ttable:", td);
        await c.createTable(td);
        console.log(Ok, n, "create table");
        await c.delete({});

        let animals = {};
        animals['horse'] = { id: 1, name: "horse"};
        animals['cat'] = { id: 2, name: "cat"};
        animals['dog'] = { id: 3, name: "dog"};
        animals['raven'] = { id: 4, name: "raven"};
        animals['cow'] = { id: 5, name: "cow"};

        for (a in animals) {
            console.log(Ok, n, "inserting row("+a+"), return id:", await c.insert(animals[a]));
            let r = await c.findOne({name: {$eq:animals[a].name}});
            console.log((r.name === animals[a].name) ? Ok : Failed, n, "findeOne("+animals[a].name+") call, result.name:", r.name);
        }

        let findArrayLength = (await c.find({}).toArray()).length;
        let findRowCount = await c.find({}).count();

        console.log((findArrayLength === findRowCount && findRowCount === 5) ? Ok : Failed, n, "compare count() and toArray().length:", findArrayLength, "and", findRowCount);

        let ra = await c.find({}).toArray();
        console.log((ra[0].name === "horse" && ra[0].id === 1 &&
                        ra[1].name === "cat" && ra[1].id === 2 &&
                        ra[2].name === "dog" && ra[2].id === 3 &&
                        ra[3].name === "raven" && ra[3].id === 4 &&
                        ra[4].name === "cow" && ra[4].id === 5) ? Ok : Failed, n, "check table elements");

        var rsa = await c.find({}).limit(2).sort({ name: 1 }).toArray();
        console.log((rsa[0].name === "cat" && rsa[0].id === 2 &&
            rsa[1].name === "cow" && rsa[1].id === 5) ? Ok : Failed, n, "check sorted ASC elements");

        var rssa = await c.find({}).skip(2).limit(2).sort({ name: -1 }).toArray();
        console.log((rssa[0].name === "dog" && rssa[0].id === 3 &&
            rssa[1].name === "cow" && rssa[1].id === 5) ? Ok : Failed, n, "check sorted DESC elements");

        await c.delete({ name: { $eq: "cow" } });
        let f1 = await c.find({ name: { $eq: "cow" } }).toArray();
        let f2 = await c.findOne({ name: { $eq: "cow" } });
        console.log((f1.length === 0 && f2 === null) ? Ok : Failed, n, "find & findOne removed row", f1, f2);

        let nrc = await c.find({}).count();
        console.log((nrc === 4) ? Ok : Failed, n, "new rows count:", nrc);

        let rc = await c.find({ name: { $eq: "raven" } }).count();
        console.log((rc === 1) ? Ok : Failed, n, "ravens count({name:{$eq: 'raven'}}) :", rc);
        let r1 = (await c.find({ name: { $eq: "raven" } }).toArray())[0];
        console.log((r1 !== null) ? Ok : Failed, n, "find raven object:");
        let r2 = await c.findOne({ name: { $eq: "raven" } });
        console.log((r2 !== null) ? Ok : Failed, n, "findOne raven object");
        console.log((r1.id === r2.id && r1.name === r2.name) ? Ok : Failed, n, "compare raven objects:");
        console.log(Ok, n, "update raven to falcon");
        await c.update({ name: { $eq: "raven"}}, { $set: { name: "falcon" }});
        let rf1 = await c.findOne({ name: { $eq: "raven" } });
        let rf2 = await c.findOne({ name: { $eq: "falcon" } });
        console.log((rf1 === null) ? Ok : Failed, n, "find raven object:", rf1);
        console.log((rf2 !== null) ? Ok : Failed, n, "find falcon object:", rf2);

        await c.dropTable();
        console.log(Ok, n, "drop table");
        resolve();
    });
}

function getMongoDbConnection(dbName, tableName, td){
    return new Promise((resolve, reject) => {

        var c = mongodb.MongoClient;       
        c.connect("mongodb://localhost:27017", function (err, client) {
            if (err) reject(err);
            let db = client.db(dbName);
            let mongoDao = new dao.mongodb(db, tableName)
            resolve({ c: mongoDao, td:td});
        });
    });
}

function getMysqlConnection(tableName, td) {
    return new Promise((resolve, reject) => {
        let c = mysql.createConnection({
            host: 'localhost',
            user: 'test',
            password: 'test',
            database: 'test'
        });

        c.connect((err) => {
            if (err) reject(err);
            let mysqlDao = new dao.mysql(c, tableName);
            resolve({c:mysqlDao, td:td});
        });
    });
}

function getPostgreSqlConnection(tableName, td) {
    return new Promise((resolve, reject) => {
        var config = {
            user: 'test',
            database: 'test',
            password: 'test',
            host: 'localhost',
            port: 5432,
            max: 10,
            idleTimeoutMillis: 30000,
        };

        var pool = new pg.Pool(config);
        pool.connect(function (err) {
            if (err) reject(err);

            let postgresqlDao = new dao.postgresql(pool, tableName);
            resolve({c:postgresqlDao, td:td});
        });
    });
}

function getSqliteConnection(tableName, td){
    return new Promise((resolve, reject) => {
        var db = new sqlite.Database();
        let sqliteDao = new dao.sqlite(db, tableName);
        resolve({c:sqliteDao, td:td});
    });
}

async function testConnnections()
{
   let dbName = "test";
   let tableName = "animals";

   var ci = {};
   ci["sqlite"] = await getSqliteConnection(tableName, { id: "INTEGER", name: "char" });
   ci["mongodb"] = await getMongoDbConnection(dbName, tableName);
   ci["mysql"] = await getMysqlConnection(tableName, { id: "INT(6) UNSIGNED AUTO_INCREMENT PRIMARY KEY", name: "TEXT"});
   ci["postgresql"] = await getPostgreSqlConnection(tableName, {id: "INTEGER PRIMARY KEY DEFAULT NEXTVAL('user_ids')", name: "TEXT"});

   for(let n in ci)
   {
       await check(n, ci[n].c, ci[n].td);
   }

   process.exit(0);
}


testConnnections();

