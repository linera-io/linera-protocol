import {
  ApolloClient,
  ApolloProvider,
  HttpLink,
  InMemoryCache,
  split,
} from "@apollo/client";
import React from "react";
import { GraphQLWsLink } from "@apollo/client/link/subscriptions";
import { createClient } from "graphql-ws";
import { getMainDefinition } from "@apollo/client/utilities";

function GraphQLProvider({ chainId, applicationId, port, children }) {
  let client = apolloClient(chainId, applicationId, port);
  return <ApolloProvider client={client}>{children}</ApolloProvider>;
}

function apolloClient(chainId, applicationId, port) {
  const currentHost = window.location.hostname;

  const wsLink = new GraphQLWsLink(
    createClient({
      url: `ws://${currentHost}:${port}/ws`,
    })
  );

  const httpLink = new HttpLink({
    uri: `http://${currentHost}:${port}/chains/${chainId}/applications/${applicationId}`,
  });

  const splitLink = split(
    ({ query }) => {
      const definition = getMainDefinition(query);
      return (
        definition.kind === "OperationDefinition" &&
        definition.operation === "subscription"
      );
    },
    wsLink,
    httpLink
  );

  return new ApolloClient({
    link: splitLink,
    cache: new InMemoryCache(),
  });
}

export default GraphQLProvider;
