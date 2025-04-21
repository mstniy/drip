This is a small demo that uses Drip to synchronize a hypothetical list of todo items from a local MongoDB instance.

Make sure you have a local MongoDB replica set running on port 27017, the default for MongoDB.

If you are using VSCode, you can use the defined task to spin up such an instance.

Then, create the target collection with the right config:

```sh
mongosh 'localhost:27017' --eval 'use drip_demo' --eval 'db.createCollection(
    "drip_demo",
    {changeStreamPreAndPostImages: { enabled: true }
});'
```

And start the persister:

`npx tsx persister.ts`

Finally, you can start the demo:

`npx tsx demo.ts`

Now you can create, modify and delete todo items in the target collection. Here is some inspiration:

```
~> mongosh drip_demo
drip_demo> db.drip_demo.insertOne({userId: "me", title: "test"}) // Insert a new item
drip_demo> db.drip_demo.insertOne({userId: "me", title: "test2"}) // Insert another item
drip_demo> db.drip_demo.updateOne({title: "test"}, {$set: {userId: "you"}}) // Lose access to an item
drip_demo> db.drip_demo.updateOne({title: "test"}, {$set: {userId: "me"}}) // Gain access to an item
drip_demo> db.drip_demo.updateOne({title: "test"}, {$set: {title: "new title"}}) // Update an item
drip_demo> db.drip_demo.deleteOne({title: "new title"}) // Delete an item
```

Note that the MongoDB documents must satisfy the schema defined in `demo.ts`.

You can create the following indices to speed up CEA queries:

```
let coll = db.getCollection("_drip_pcs_v1_drip_demo")
coll.createIndex({ct: 1})
coll.createIndex({"a.userId":1, ct:1, _id: 1}, {name: 'for_ins', partialFilterExpression: {o: "i"}})
coll.createIndex({"o":1, "b.userId":1, ct:1, _id: 1}, {partialFilterExpression: {o: {$in: ["d", "u"]}}})
coll.createIndex({"o":1, "a.userId":1, ct:1, _id: 1}, {partialFilterExpression: {o: "u"}})
```

And similarly for the CC queries:

```
db.drip_demo.createIndex({userId: 1, _id: 1})
```
