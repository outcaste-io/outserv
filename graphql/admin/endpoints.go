// Portions Copyright 2020 Dgraph Labs, Inc. are available under the Apache License v2.0.
// Portions Copyright 2022 Outcaste LLC are available under the Smart License v1.0.

package admin

const adminTypes = ``

const adminMutations = ``

const adminQueries = ``

// GraphQL schema for /admin endpoint.
const graphqlAdminSchema = `
	"""
	The Int64 scalar type represents a signed 64‐bit numeric non‐fractional value.
	Int64 can represent values in range [-(2^63),(2^63 - 1)].
	"""
	scalar Int64

    """
	The UInt64 scalar type represents an unsigned 64‐bit numeric non‐fractional value.
	UInt64 can represent values in range [0,(2^64 - 1)].
	"""
    scalar UInt64

	"""
	The DateTime scalar type represents date and time as a string in RFC3339 format.
	For example: "1985-04-12T23:20:50.52Z" represents 20 minutes and 50.52 seconds after the 23rd hour of April 12th, 1985 in UTC.
	"""
	scalar DateTime

	"""
	File Upload
	"""
	scalar Upload

	"""
	Data about the GraphQL schema being served by Dgraph.
	"""
	type GQLSchema @dgraph(type: "dgraph.graphql") {
		id: ID!

		"""
		Input schema (GraphQL types) that was used in the latest schema update.
		"""
		schema: String!  @dgraph(pred: "dgraph.graphql.schema")

		"""
		The GraphQL schema that was generated from the 'schema' field.
		This is the schema that is being served by Dgraph at /graphql.
		"""
		generatedSchema: String!
	}

	"""
	Data about the Lambda script served by Dgraph.
	"""
	type LambdaScript @dgraph(type: "dgraph.graphql.lambda") {
		id: ID!
		"""
		Input script (multi-part binary)
		"""
		script: Upload @dgraph(pred: "dgraph.graphql.lambda.script")
		hash: String! @dgraph(pred: "dgraph.graphql.lambda.hash")
	}

	"""
	A NodeState is the state of an individual node in the Dgraph cluster.
	"""
	type NodeState {

		"""
		Node type : either 'alpha' or 'zero'.
		"""
		instance: String

		"""
		Address of the node.
		"""
		address: String

		"""
		Node health status : either 'healthy' or 'unhealthy'.
		"""
		status: String

		"""
		The group this node belongs to in the Dgraph cluster.
		See : https://dgraph.io/docs/deploy/#cluster-setup.
		"""
		group: String

		"""
		Version of the Dgraph binary.
		"""
		version: String

		"""
		Time in nanoseconds since the node started.
		"""
		uptime: Int64

		"""
		Time in Unix epoch time that the node was last contacted by another Zero or Alpha node.
		"""
		lastEcho: Int64

		"""
		List of ongoing operations in the background.
		"""
		ongoing: [String]

		"""
		List of predicates for which indexes are built in the background.
		"""
		indexing: [String]

		"""
		List of Enterprise Features that are enabled.
		"""
		ee_features: [String]
	}

	type MembershipState {
		counter: UInt64
		groups: [ClusterGroup]
		zeros: [Member]
		maxUID: UInt64
		maxNsID: UInt64
		maxTxnTs: UInt64
		maxRaftId: UInt64
		removed: [Member]
		cid: String
		license: License
		"""
		Contains list of namespaces. Note that this is not stored in proto's MembershipState and
		computed at the time of query.
		"""
		namespaces: [UInt64]
	}

	type ClusterGroup {
		id: UInt64
		members: [Member]
		tablets: [Tablet]
		snapshotTs: UInt64
		checksum: UInt64
	}

	type Member {
		id: UInt64
		groupId: UInt64
		addr: String
		leader: Boolean
		amDead: Boolean
		lastUpdate: UInt64
		clusterInfoOnly: Boolean
		forceGroupId: Boolean
	}

	type Tablet {
		groupId: UInt64
		predicate: String
		force: Boolean
		space: Int
		remove: Boolean
		readOnly: Boolean
		moveTs: UInt64
	}

	type License {
		user: String
		maxNodes: UInt64
		expiryTs: Int64
		enabled: Boolean
	}

	directive @dgraph(type: String, pred: String) on OBJECT | INTERFACE | FIELD_DEFINITION
	directive @id on FIELD_DEFINITION
	directive @secret(field: String!, pred: String) on OBJECT | INTERFACE


	type UpdateGQLSchemaPayload {
		gqlSchema: GQLSchema
	}

	input UpdateGQLSchemaInput {
		set: GQLSchemaPatch!
	}

	input GQLSchemaPatch {
		schema: String!
	}

	type UpdateLambdaScriptPayload {
		lambdaScript: LambdaScript
	}

	input UpdateLambdaScriptInput {
		set: ScriptPatch!
	}

	input ScriptPatch {
		script: Upload!
	}

	input ExportInput {
		"""
		Data format for the export, e.g. "json" (default: "json")
		"""
		format: String

		"""
		Namespace for the export in multi-tenant cluster. Users from guardians of galaxy can export
		all namespaces by passing a negative value or specific namespaceId to export that namespace.
		"""
		namespace: Int

		"""
		Destination for the export: e.g. Minio or S3 bucket or /absolute/path
		"""
		destination: String

		"""
		Access key credential for the destination.
		"""
		accessKey: String

		"""
		Secret key credential for the destination.
		"""
		secretKey: String

		"""
		AWS session token, if required.
		"""
		sessionToken: String

		"""
		Set to true to allow backing up to S3 or Minio bucket that requires no credentials.
		"""
		anonymous: Boolean
	}

	input TaskInput {
		id: String!
	}

	type Response {
		code: String
		message: String
	}

	type ExportPayload {
		response: Response
		taskId: String
	}

	type DrainingPayload {
		response: Response
	}

	type ShutdownPayload {
		response: Response
	}

	type TaskPayload {
		kind: TaskKind
		status: TaskStatus
		lastUpdated: DateTime
	}

	enum TaskStatus {
		Queued
		Running
		Failed
		Success
		Unknown
	}

	enum TaskKind {
		Export
		Unknown
	}

	input ConfigInput {
		"""
		Estimated memory the caches can take. Actual usage by the process would be
		more than specified here. The caches will be updated according to the
		cache_percentage flag.
		"""
		cacheMb: Float

		"""
		True value of logRequest enables logging of all the requests coming to alphas.
		False value of logRequest disables above.
		"""
		logRequest: Boolean
	}

	type ConfigPayload {
		response: Response
	}

	type Config {
		cacheMb: Float
	}

	input RemoveNodeInput {
		"""
		ID of the node to be removed.
		"""
		nodeId: UInt64!

		"""
		ID of the group from which the node is to be removed.
		"""
		groupId: UInt64!
	}

	type RemoveNodePayload {
		response: Response
	}

	input MoveTabletInput {
		"""
		Namespace in which the predicate exists.
		"""
		namespace: UInt64

		"""
		Name of the predicate to move.
		"""
		tablet: String!

		"""
		ID of the destination group where the predicate is to be moved.
		"""
		groupId: UInt64!
	}

	type MoveTabletPayload {
		response: Response
	}

	enum AssignKind {
		UID
		TIMESTAMP
		NAMESPACE_ID
	}

	input AssignInput {
		"""
		Choose what to assign: UID, TIMESTAMP or NAMESPACE_ID.
		"""
		what: AssignKind!

		"""
		How many to assign.
		"""
		num: UInt64!
	}

	type AssignedIds {
		"""
		The first UID, TIMESTAMP or NAMESPACE_ID assigned.
		"""
		startId: UInt64

		"""
		The last UID, TIMESTAMP or NAMESPACE_ID assigned.
		"""
		endId: UInt64

		"""
		TIMESTAMP for read-only transactions.
		"""
		readOnly: UInt64
	}

	type AssignPayload {
		response: AssignedIds
	}


	` + adminTypes + `

	type Query {
		getGQLSchema: GQLSchema
		getLambdaScript: LambdaScript
		health: [NodeState]
		state: MembershipState
		config: Config
		task(input: TaskInput!): TaskPayload
		` + adminQueries + `
	}

	type Mutation {

		"""
		Update the Dgraph cluster to serve the input schema.  This may change the GraphQL
		schema, the types and predicates in the Dgraph schema, and cause indexes to be recomputed.
		"""
		updateGQLSchema(input: UpdateGQLSchemaInput!) : UpdateGQLSchemaPayload

		"""
		Update the lambda script used by lambda resolvers.
		"""
		updateLambdaScript(input: UpdateLambdaScriptInput!) : UpdateLambdaScriptPayload

		"""
		Starts an export of all data in the cluster.  Export format should be 'rdf' (the default
		if no format is given), or 'json'.
		See : https://dgraph.io/docs/deploy/#export-database
		"""
		export(input: ExportInput!): ExportPayload

		"""
		Set (or unset) the cluster draining mode.  In draining mode no further requests are served.
		"""
		draining(enable: Boolean): DrainingPayload

		"""
		Shutdown this node.
		"""
		shutdown: ShutdownPayload

		"""
		Alter the node's config.
		"""
		config(input: ConfigInput!): ConfigPayload

		"""
		Remove a node from the cluster.
		"""
		removeNode(input: RemoveNodeInput!): RemoveNodePayload

		"""
		Move a predicate from one group to another.
		"""
		moveTablet(input: MoveTabletInput!): MoveTabletPayload

		"""
		Lease UIDs, Timestamps or Namespace IDs in advance.
		"""
		assign(input: AssignInput!): AssignPayload


		` + adminMutations + `
	}
 `
