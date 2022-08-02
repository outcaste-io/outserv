// Portions Copyright 2021 Dgraph Labs, Inc. are available under the Apache License v2.0.
// Portions Copyright 2022 Outcaste LLC are available under the Sustainable License v1.0.

import fetch from 'node-fetch';
import { GraphQLResponse, AuthHeaderField } from '@dgraph-lambda/lambda-types';

export async function graphql(query: string, variables: Record<string, any> = {}, authHeader?: AuthHeaderField, accessToken?: string): Promise<GraphQLResponse> {
  const headers: Record<string, string> = { "Content-Type": "application/json" };
  if (authHeader && authHeader.key && authHeader.value) {
    headers[authHeader.key] = authHeader.value;
  }
  headers['X-Dgraph-AccessToken'] = accessToken || ""
  const response = await fetch(`${process.env.DGRAPH_URL}/graphql`, {
    method: "POST",
    headers,
    body: JSON.stringify({ query, variables })
  })
  if (response.status !== 200) {
    throw new Error("Failed to execute GraphQL Query")
  }
  return response.json();
}

async function dqlQuery(query: string, variables: Record<string, any> = {}, accessToken?:string): Promise<GraphQLResponse> {
  const response = await fetch(`${process.env.DGRAPH_URL}/query`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "X-Dgraph-AccessToken": accessToken || "",
    },
    body: JSON.stringify({ query, variables })
  })
  if (response.status !== 200) {
    throw new Error("Failed to execute DQL Query")
  }
  return response.json();
}

export const dql = {
  query: dqlQuery,
}
