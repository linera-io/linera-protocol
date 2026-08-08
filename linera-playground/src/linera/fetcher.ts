import type { GraphQLRequest, LineraClientManager, QueryTarget } from './clientManager';

/** A GraphQL response as rendered in the editor's result pane. */
export interface GraphQLResult {
  data?: unknown;
  errors?: { message: string }[];
}

/** The custom fetcher the GraphiQL editor calls for every operation. */
export type PlaygroundFetcher = (params: {
  query: string;
  variables?: Record<string, unknown> | null;
  operationName?: string | null;
}) => Promise<GraphQLResult>;

/**
 * Adapts the WASM client to GraphiQL's fetcher contract: it reads the current
 * target at call time, routes the operation through `LineraClientManager`, and
 * maps thrown errors to a GraphQL error payload so they render in the result
 * pane instead of crashing the app.
 */
export function makeFetcher(
  manager: LineraClientManager,
  getTarget: () => Partial<QueryTarget>,
): PlaygroundFetcher {
  return async ({ query, variables, operationName }) => {
    const { faucetUrl, chainId, appId } = getTarget();
    if (!faucetUrl || !chainId || !appId) {
      return {
        errors: [
          { message: 'Set the faucet URL, chain ID, and application ID, then run your query.' },
        ],
      };
    }
    const request: GraphQLRequest = { query, variables, operationName };
    try {
      return (await manager.query({ faucetUrl, chainId, appId }, request)) as GraphQLResult;
    } catch (err) {
      return { errors: [{ message: err instanceof Error ? err.message : String(err) }] };
    }
  };
}
