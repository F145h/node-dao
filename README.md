# node-dao

Node-Dao - Simple MongoDb-like interface to MySql, PostgreSql, SqlLite and Mongo databases




look like this:
```js

var rsa = await daoConnection.find({}).limit(2).sort({ name: 1 }).toArray();

```



find method samples: 

```js

find({value: {$gte: 25}})

find({value: {$ne: 4}})

find({age: {$gt: 20, $lt:10}})

find({age: {$gt: 20, $lt:10}})

find({age: {$gt: 20}, name: {$eq: “jenny”}});

```
so, dao request 

```js
find({name: {$eq: “john”}}).limit(1).sort({time: -1}).limit(1).toArray();
```

will be translated for MySQL  like that

```js
SELECT * FROM messages WHERE name = “john” ORDER BY name DESC LIMIT 1 OFFSET 1;
```

If you call count() instread toArray() then the request will be generated as:

```js
SELECT count(*) FROM messages WHERE name = “john” LIMIT 1 OFFSET 1;
```




