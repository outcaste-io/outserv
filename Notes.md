### Manish

[x] Ensure that the read timestamp for a query is the latest
  commit ts + 1. This is because rollups set the value to the latest write
  ts + 1.

[ ] Ensure that Zero snapshots and Alpha snapshots work.

### Type System

Use type level UIDs.

### GraphQL Mutations

[ ] Check that an update mutation won't cause duplicate XIDs.
[ ] Deal with nested objects and their upserts.

