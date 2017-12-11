let dao = require('../index.js');

let mysql = require("mysql");
let pg = require("pg");
let mongodb = require("mongodb");
let sqlite = require("sql.js");



async function check(n, c) {
console.log("test for database", n);
    await c.delete({});

    let animals = {};
    animals['horse'] = { id: 1, name: "horse"};
    animals['cat'] = { id: 2, name: "cat"};
    animals['dog'] = { id: 3, name: "dog"};
    animals['raven'] = { id: 4, name: "raven"};
    animals['cow'] = { id: 5, name: "cow"};

    for (a in animals) {
        console.log(n, "insert", animals[a]);
        await c.insert(animals[a]);
        console.log(n, "findOne", animals[a].name);
        let r = await c.findOne({name: {$eq:animals[a].name}});
        console.log(n, "result:", r);
    }

    let ra = await c.find({}).toArray();
    console.log(ra);

    var rsa = await c.find({}).limit(2).sort({ name: 1 }).toArray();
    console.log(rsa);

    var rssa = await c.find({}).skip(2).limit(2).sort({ name: -1 }).toArray();
    console.log(rssa);

    await c.delete({ name: { $eq: "cow" } });
    let f1 = await c.find({ name: { $eq: "cow" } }).toArray();
    let f2 = await c.findOne({ name: { $eq: "cow" } });
    console.log(f1, f2);
    
return;

    console.log(n, result);
    console.log(n, 'update({ $set : { name: "horse1" } }, { name: { $eq: "horse" } }');
    await c.update({ $set: { name: "horse1" } }, { name: { $eq: "horse" } });

    console.log(n, 'delete({name: {$eq: "cow"}}');
    await c.delete({ name: { $eq: "cow" } });

    console.log(n, 'find({}).sort({ name: 1 }');
    result = await c.find({}).sort({ name: 1 }).toArray();

    console.log(n, result);
    console.log('');
    await c.delete({})
}

function getMongoDbConnection(){
   return new Promise((resolve, reject) => {
       var c = mongodb.MongoClient;
       c.connect("mongodb://localhost:27017/test", function (err, db) {
            if (err) reject(err);
            let mongodbDao = new dao.mongodb(db, "animals");
            resolve(mongodbDao);
        });
    });
}

function getMysqlConnection(tableName) {
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
            resolve(mysqlDao);
        });
    });
}

function getPostgreSqlConnection(tableName) {
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
            resolve(postgresqlDao);
        });
    });
}

function getSqliteConnection(tableName){
    return new Promise((resolve, reject) => {
        var db = new sqlite.Database();
        db.run("CREATE TABLE animals (id int, name char);");

        let sqliteDao = new dao.sqlite(db, tableName);
        resolve(sqliteDao);
    });
}

async function testConnnections()
{
   let tableName = "animals";

   var c = {};
   c["sqlite"] = await getSqliteConnection(tableName);
   c["mysql"] = await getMysqlConnection(tableName);
   c["postgresql"] = await getPostgreSqlConnection(tableName);
   c["mongodb"] = await getMongoDbConnection(tableName);

   for(let n in c)
   {
        check(n, c[n]);
   }
}


testConnnections();

