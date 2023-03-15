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

function GraphQLProvider({ applicationId, children }) {
  let client = apolloClient(applicationId);
  return <ApolloProvider client={client}>{children}</ApolloProvider>;
}

function apolloClient(applicationId) {
  const wsLink = new GraphQLWsLink(
    createClient({
      url: "ws://localhost:8080/ws",
    })
  );

  const httpLink = new HttpLink({
    uri: "http://localhost:8080/applications/" + applicationId,
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
