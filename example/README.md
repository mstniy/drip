This is a small demo that uses Drip to synchronize a hypothetical list of todo items from a local MongoDB instance.

Make sure you have a local MongoDB instance running on port 27017 (the default for MongoDB).

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
drip_demo> db.drip_demo.insert({userId: "me", title: "test"}) // Insert a new item
drip_demo> db.drip_demo.insert({userId: "me", title: "test2"}) // Insert another item
drip_demo> db.drip_demo.updateOne({title: "test"}, {$set: {userId: "you"}}) // Lose access to an item
drip_demo> db.drip_demo.updateOne({title: "test"}, {$set: {userId: "me"}}) // Gain access to an item
drip_demo> db.drip_demo.updateOne({title: "test"}, {$set: {title: "new title"}}) // Update an item
drip_demo> db.drip_demo.deleteOne({title: "new title"}) // Delete an item
```

Note that the MongoDB documents must satisfy the schema defined in `demo.ts`.
