# GraphQL Module

## Schema

Schema logic is in schema/gqlschema.go.


## Mutation Logic

### Add

For example, a GraphQL add mutation to add an object of type Author,
with GraphQL input object (where country code is @id)

```
{
  name: "A.N. Author",
  country: { code: "ind", name: "India" },
  posts: [ { title: "A Post", text: "Some text" }]
  friends: [ { id: "0x123" } ]
}
```

This query will be executed and depending on the result it would be decided whether
to create a new country as part of this mutation or link it to an existing country.
If it is found out that there is an existing country, no modifications are made to
the country's attributes and its children. Mutations of the country's children are
simply ignored.

If it is found out that the Person with id 0x123 does not exist, the corresponding
mutation will fail.

### Delete

The GraphQL updates look like:

```
input UpdateAuthorInput {
  filter: AuthorFilter!
  set: PatchAuthor
  remove: PatchAuthor
}
```

The semantics is the same as the Dgraph mutation semantics.
- Any values in set become the new values for those predicates (or add to the existing
  values for lists)
- Any nulls in set are ignored.
- Explicit values in remove mean delete this if it is the actual value
- Nulls in remove become like delete * for the corresponding predicate.

## Files

These are the list of files which have logic.

```
$ tree -I "*.yaml|*.yml|*.graphql|*_test.go|testdata|*.json|*.pem|test"
```

