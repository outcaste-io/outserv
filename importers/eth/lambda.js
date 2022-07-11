async function accountBal({args, graphql, dql}) {
  console.log("args: ", args);
  const results = await dql.query(`
query q($hash: string, $block: int) {
  var(func: eq(Account.Hash, $hash)) {
    Account.Outgoing { ov as Txn.Value }
    Account.Incoming { iv as Txn.Value }
  }
  var() {
    in as sum(val(iv))
    out as sum(val(ov))
  }
  bal() {
    Value: math(in - out)
  }
}`, {"$hash": args.Hash, "$block": args.Block})
  console.log(results.data);
  return results.data.bal[0]
}

async function test({args, graphql, dql}) {
  return "hey there, I'm just Manish. this is a test"
}

self.addGraphQLResolvers({
  "Query.Balance": accountBal,
  "Query.Test": test
})

