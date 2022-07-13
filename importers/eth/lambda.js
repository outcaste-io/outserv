async function accountBal({args, graphql, dql}) {
  console.log("accountBal args: ", args);
  const results = await graphql(`
  query q($address: String) {
    queryAccount(filter: {address: {eq: $address}}) {
      incomingAggregate { valueSum }
      outgoingAggregate { valueSum }
    }
  }`, {"address": args.address })

  console.log("accountBal data:", results.data);
  if (results.data.queryAccount.length == 0) {
    return {"Value": 0}
  }
  acc = results.data.queryAccount[0]
  console.log("accountBal:", acc);
  inc = acc.incomingAggregate.valueSum;
  if (inc == null) {
    inc = 0;
  }
  out = acc.outgoingAggregate.valueSum;
  if (out == null) {
    out = 0;
  }
  diff = inc - out
  console.log("accountBal diff:", diff);
  return {"address": args.address, "value": diff}
}

async function test({args, graphql, dql}) {
  return "hey there, I'm just Manish. this is a test"
}

async function latestBlock({args, graphql}) {
  const results = await graphql(`
  { queryBlock(order: {desc: number}, first: 1) {
    number
  }} `)
  if (results.data.queryBlock.length == 0) {
    return 0
  }
  return results.data.queryBlock[0].number;
}

self.addGraphQLResolvers({
  "Query.accountBalance": accountBal,
  "Query.test": test,
  "Query.latestBlock": latestBlock
})

