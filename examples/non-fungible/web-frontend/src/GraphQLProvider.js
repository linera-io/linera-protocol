import {
  ApolloClient,
  ApolloProvider,
  HttpLink,
  InMemoryCache,
  split,
} from "@apollo/client";
import React, { createContext, useContext } from "react";
import { GraphQLWsLink } from "@apollo/client/link/subscriptions";
import { createClient } from "graphql-ws";
import { getMainDefinition } from "@apollo/client/utilities";

// Create a context for selecting the client
const ClientContext = createContext();

export function useClient() {
  return useContext(ClientContext);
}

function GraphQLProvider({ chainId, applicationId, port, children }) {
  const appClient = createAppClient(chainId, applicationId, port);
  const nodeServiceClient = createNodeServiceClient(port);

  const value = {
    appClient,
    nodeServiceClient,
  };

  return (
    <ClientContext.Provider value={value}>
      <ApolloProvider client={appClient}>{children}</ApolloProvider>
    </ClientContext.Provider>
  );
}

function createAppClient(chainId, applicationId, port) {
  const wsLink = new GraphQLWsLink(
    createClient({
      url: `ws://localhost:${port}/ws`,
    })
  );

  const httpLink = new HttpLink({
    uri: `http://localhost:${port}/chains/${chainId}/applications/${applicationId}`,
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

function createNodeServiceClient(port) {
  const wsLink = new GraphQLWsLink(
    createClient({
      url: `ws://localhost:${port}/ws`,
    })
  );

  const httpLink = new HttpLink({
    uri: `http://localhost:${port}/`,
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
