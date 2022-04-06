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
[ ] NumUids in deletion operation doesn't return any value.
[ ] Without upsert, we seem to be adding duplicate records.

#### Inverse

[ ] Addition works with inverse.
[ ] Deletion works with inverse.
[ ] Update works with inverse.

### Performance

[ ] Build a cache system, which can retrieve objects concurrently to aid with mutations.

### Posting List

[ ] Separate out values from UIDs. Posting shouldn't be storing both.
[ ] Don't use value hashes as UIDs for postings.
