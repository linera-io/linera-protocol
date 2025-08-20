/* eslint-disable */
import { TypedDocumentNode as DocumentNode } from '@graphql-typed-document-node/core';
export type Maybe<T> = T | null;
export type InputMaybe<T> = Maybe<T>;
export type Exact<T extends { [key: string]: unknown }> = { [K in keyof T]: T[K] };
export type MakeOptional<T, K extends keyof T> = Omit<T, K> & { [SubKey in K]?: Maybe<T[SubKey]> };
export type MakeMaybe<T, K extends keyof T> = Omit<T, K> & { [SubKey in K]: Maybe<T[SubKey]> };
export type MakeEmpty<T extends { [key: string]: unknown }, K extends keyof T> = { [_ in K]?: never };
export type Incremental<T> = T | { [P in keyof T]?: P extends ' $fragmentName' | '__typename' ? T[P] : never };
/** All built-in and custom scalars, mapped to their actual values */
export type Scalars = {
  ID: { input: string; output: string; }
  String: { input: string; output: string; }
  Boolean: { input: boolean; output: boolean; }
  Int: { input: number; output: number; }
  Float: { input: number; output: number; }
  /** An owner of an account. */
  AccountOwner: { input: any; output: any; }
  /** A non-negative amount of tokens. */
  Amount: { input: any; output: any; }
  /** The unique identifier (UID) of a chain. This is currently computed as the hash value of a ChainDescription. */
  ChainId: { input: any; output: any; }
};

/** A GraphQL-visible map item, complete with key. */
export type Entry_AccountOwner_Amount_92cf94e6 = {
  __typename?: 'Entry_AccountOwner_Amount_92cf94e6';
  key: Scalars['AccountOwner']['output'];
  value?: Maybe<Scalars['Amount']['output']>;
};

/** An account. */
export type FungibleAccount = {
  /** Chain ID of the account */
  chainId: Scalars['ChainId']['input'];
  /** Owner of the account */
  owner: Scalars['AccountOwner']['input'];
};

export type FungibleTokenService = {
  __typename?: 'FungibleTokenService';
  accounts: MapView_AccountOwner_Amount_Ea496e19;
  tickerSymbol: Scalars['String']['output'];
};

export type MapFilters_AccountOwner_2fb690f5 = {
  keys?: InputMaybe<Array<Scalars['AccountOwner']['input']>>;
};

export type MapInput_AccountOwner_30d769cb = {
  filters?: InputMaybe<MapFilters_AccountOwner_2fb690f5>;
};

export type MapView_AccountOwner_Amount_Ea496e19 = {
  __typename?: 'MapView_AccountOwner_Amount_ea496e19';
  entries: Array<Entry_AccountOwner_Amount_92cf94e6>;
  entry: Entry_AccountOwner_Amount_92cf94e6;
  keys: Array<Scalars['AccountOwner']['output']>;
};


export type MapView_AccountOwner_Amount_Ea496e19EntriesArgs = {
  input?: InputMaybe<MapInput_AccountOwner_30d769cb>;
};


export type MapView_AccountOwner_Amount_Ea496e19EntryArgs = {
  key: Scalars['AccountOwner']['input'];
};


export type MapView_AccountOwner_Amount_Ea496e19KeysArgs = {
  count?: InputMaybe<Scalars['Int']['input']>;
};

export type OperationMutationRoot = {
  __typename?: 'OperationMutationRoot';
  balance: Array<Scalars['Int']['output']>;
  claim: Array<Scalars['Int']['output']>;
  tickerSymbol: Array<Scalars['Int']['output']>;
  transfer: Array<Scalars['Int']['output']>;
};


export type OperationMutationRootBalanceArgs = {
  owner: Scalars['AccountOwner']['input'];
};


export type OperationMutationRootClaimArgs = {
  amount: Scalars['Amount']['input'];
  sourceAccount: FungibleAccount;
  targetAccount: FungibleAccount;
};


export type OperationMutationRootTransferArgs = {
  amount: Scalars['Amount']['input'];
  owner: Scalars['AccountOwner']['input'];
  targetAccount: FungibleAccount;
};

export type AccountsQueryVariables = Exact<{
  owner: Scalars['AccountOwner']['input'];
}>;


export type AccountsQuery = { __typename?: 'FungibleTokenService', accounts: { __typename?: 'MapView_AccountOwner_Amount_ea496e19', entry: { __typename?: 'Entry_AccountOwner_Amount_92cf94e6', value?: any | null } } };

export type TickerSymbolQueryVariables = Exact<{ [key: string]: never; }>;


export type TickerSymbolQuery = { __typename?: 'FungibleTokenService', tickerSymbol: string };

export type TransferMutationVariables = Exact<{
  owner: Scalars['AccountOwner']['input'];
  amount: Scalars['Amount']['input'];
  targetAccount: FungibleAccount;
}>;


export type TransferMutation = { __typename?: 'OperationMutationRoot', transfer: Array<number> };


export const AccountsDocument = {"kind":"Document","definitions":[{"kind":"OperationDefinition","operation":"query","name":{"kind":"Name","value":"Accounts"},"variableDefinitions":[{"kind":"VariableDefinition","variable":{"kind":"Variable","name":{"kind":"Name","value":"owner"}},"type":{"kind":"NonNullType","type":{"kind":"NamedType","name":{"kind":"Name","value":"AccountOwner"}}}}],"selectionSet":{"kind":"SelectionSet","selections":[{"kind":"Field","name":{"kind":"Name","value":"accounts"},"selectionSet":{"kind":"SelectionSet","selections":[{"kind":"Field","name":{"kind":"Name","value":"entry"},"arguments":[{"kind":"Argument","name":{"kind":"Name","value":"key"},"value":{"kind":"Variable","name":{"kind":"Name","value":"owner"}}}],"selectionSet":{"kind":"SelectionSet","selections":[{"kind":"Field","name":{"kind":"Name","value":"value"}}]}}]}}]}}]} as unknown as DocumentNode<AccountsQuery, AccountsQueryVariables>;
export const TickerSymbolDocument = {"kind":"Document","definitions":[{"kind":"OperationDefinition","operation":"query","name":{"kind":"Name","value":"TickerSymbol"},"selectionSet":{"kind":"SelectionSet","selections":[{"kind":"Field","name":{"kind":"Name","value":"tickerSymbol"}}]}}]} as unknown as DocumentNode<TickerSymbolQuery, TickerSymbolQueryVariables>;
export const TransferDocument = {"kind":"Document","definitions":[{"kind":"OperationDefinition","operation":"mutation","name":{"kind":"Name","value":"Transfer"},"variableDefinitions":[{"kind":"VariableDefinition","variable":{"kind":"Variable","name":{"kind":"Name","value":"owner"}},"type":{"kind":"NonNullType","type":{"kind":"NamedType","name":{"kind":"Name","value":"AccountOwner"}}}},{"kind":"VariableDefinition","variable":{"kind":"Variable","name":{"kind":"Name","value":"amount"}},"type":{"kind":"NonNullType","type":{"kind":"NamedType","name":{"kind":"Name","value":"Amount"}}}},{"kind":"VariableDefinition","variable":{"kind":"Variable","name":{"kind":"Name","value":"targetAccount"}},"type":{"kind":"NonNullType","type":{"kind":"NamedType","name":{"kind":"Name","value":"FungibleAccount"}}}}],"selectionSet":{"kind":"SelectionSet","selections":[{"kind":"Field","name":{"kind":"Name","value":"transfer"},"arguments":[{"kind":"Argument","name":{"kind":"Name","value":"owner"},"value":{"kind":"Variable","name":{"kind":"Name","value":"owner"}}},{"kind":"Argument","name":{"kind":"Name","value":"amount"},"value":{"kind":"Variable","name":{"kind":"Name","value":"amount"}}},{"kind":"Argument","name":{"kind":"Name","value":"targetAccount"},"value":{"kind":"Variable","name":{"kind":"Name","value":"targetAccount"}}}]}]}}]} as unknown as DocumentNode<TransferMutation, TransferMutationVariables>;
