async function accountBal({args, graphql, dql}) {
  if (args.blockNumber == 0) {
    // TODO: This can be optimized by removing the filter altogether.
    args.blockNumber = 1000000
  }

  const results = await graphql(`
  query q($address: String, $blockNumber: Int64) {
    queryAccount(filter: {address: {eq: $address}}) {
      incomingAggregate(filter: {blockNumber: {le: $blockNumber}}) { valueSum }
      outgoingAggregate(filter: {blockNumber: {le: $blockNumber}}) { valueSum }
    }
  }`, {"address": args.address, "blockNumber": args.blockNumber })

  if (results.data.queryAccount.length == 0) {
    return {"address": args.address, "value": 0}
  }
  acc = results.data.queryAccount[0]
  inc = acc.incomingAggregate.valueSum;
  if (inc == null) {
    inc = 0;
  }
  out = acc.outgoingAggregate.valueSum;
  if (out == null) {
    out = 0;
  }
  diff = inc - out
  return {"address": args.address, "value": diff}
}

async function test({args, graphql, dql}) {
  return "Hello, World! This is a test."
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

