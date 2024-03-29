### Manish

[x] Ensure that the read timestamp for a query is the latest
  commit ts + 1. This is because rollups set the value to the latest write
  ts + 1.

[ ] Ensure that Zero snapshots and Alpha snapshots work.

### Type System

[ ] Rewrite type system. Perhaps using type level UIDs.

### GraphQL Mutations

[x] Check that an update mutation won't cause duplicate XIDs.
[x] Deal with nested objects and their upserts.
[x] NumUids in deletion operation doesn't return any value.
[ ] Without upsert, we seem to be adding duplicate records.

#### Inverse

[x] Addition works with inverse.
[x] Deletion works with inverse.
[x] Update works with inverse.
[x] A new user, assigned to an existing task doesn't correctly remove the task
    from the older user.

### Performance

[ ] Build a cache system, which can retrieve objects concurrently to aid with mutations.

### Posting List

[ ] Separate out values from UIDs. Posting shouldn't be storing both.
[ ] Don't use value hashes as UIDs for postings.

### Deletions

[ ] This doesn't work right now. Fix it: `<subject:"0x2466c53" predicate:"Account.Outgoing" object_value:"\t_STAR_ALL" op:DEL >`

### Badger

[ ] Have a debug endpoint to inspect Badger table overlaps.
