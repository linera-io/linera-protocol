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
  /** The unique identifier (UID) of a chain. This is currently computed as the hash value of a ChainDescription. */
  ChainId: { input: any; output: any; }
  /** A timestamp, in microseconds since the Unix epoch */
  Timestamp: { input: any; output: any; }
};

/** A comment on a post */
export type Comment = {
  __typename?: 'Comment';
  /** The ChainId of the commenter */
  chainId: Scalars['ChainId']['output'];
  /** The comment text */
  text: Scalars['String']['output'];
};

export type CustomMapView_Key_Post = {
  __typename?: 'CustomMapView_Key_Post';
  entries: Array<Entry_Key_Post_1d4d0dae>;
  entry: Entry_Key_Post_1d4d0dae;
  keys: Array<Key>;
};


export type CustomMapView_Key_PostEntriesArgs = {
  input?: InputMaybe<MapInput_KeyInput_7509ae38>;
};


export type CustomMapView_Key_PostEntryArgs = {
  key: KeyInput;
};


export type CustomMapView_Key_PostKeysArgs = {
  count?: InputMaybe<Scalars['Int']['input']>;
};

/** A GraphQL-visible map item, complete with key. */
export type Entry_Key_Post_1d4d0dae = {
  __typename?: 'Entry_Key_Post_1d4d0dae';
  key: Key;
  value?: Maybe<Post>;
};

/** A key by which a post is indexed. */
export type Key = {
  __typename?: 'Key';
  /** The owner of the chain on which the `Post` operation was included. */
  author: Scalars['ChainId']['output'];
  /** The number of posts by that author before this one. */
  index: Scalars['Int']['output'];
  /** The timestamp of the block in which the post was included on the author's chain. */
  timestamp: Scalars['Timestamp']['output'];
};

/** A key by which a post is indexed. */
export type KeyInput = {
  /** The owner of the chain on which the `Post` operation was included. */
  author: Scalars['ChainId']['input'];
  /** The number of posts by that author before this one. */
  index: Scalars['Int']['input'];
  /** The timestamp of the block in which the post was included on the author's chain. */
  timestamp: Scalars['Timestamp']['input'];
};

export type LogView_OwnPost_D65e13e4 = {
  __typename?: 'LogView_OwnPost_d65e13e4';
  entries: Array<OwnPost>;
};


export type LogView_OwnPost_D65e13e4EntriesArgs = {
  end?: InputMaybe<Scalars['Int']['input']>;
  start?: InputMaybe<Scalars['Int']['input']>;
};

export type MapFilters_KeyInput_20f6e4f6 = {
  keys?: InputMaybe<Array<KeyInput>>;
};

export type MapInput_KeyInput_7509ae38 = {
  filters?: InputMaybe<MapFilters_KeyInput_20f6e4f6>;
};

export type OperationMutationRoot = {
  __typename?: 'OperationMutationRoot';
  comment: Array<Scalars['Int']['output']>;
  like: Array<Scalars['Int']['output']>;
  post: Array<Scalars['Int']['output']>;
  subscribe: Array<Scalars['Int']['output']>;
  unsubscribe: Array<Scalars['Int']['output']>;
};


export type OperationMutationRootCommentArgs = {
  comment: Scalars['String']['input'];
  key: KeyInput;
};


export type OperationMutationRootLikeArgs = {
  key: KeyInput;
};


export type OperationMutationRootPostArgs = {
  imageUrl?: InputMaybe<Scalars['String']['input']>;
  text: Scalars['String']['input'];
};


export type OperationMutationRootSubscribeArgs = {
  chainId: Scalars['ChainId']['input'];
};


export type OperationMutationRootUnsubscribeArgs = {
  chainId: Scalars['ChainId']['input'];
};

/** A post's text and timestamp, to use in contexts where author and index are known. */
export type OwnPost = {
  __typename?: 'OwnPost';
  /** The posted Image_url(optional). */
  imageUrl?: Maybe<Scalars['String']['output']>;
  /** The posted text. */
  text: Scalars['String']['output'];
  /** The timestamp of the block in which the post operation was included. */
  timestamp: Scalars['Timestamp']['output'];
};

/** A post on the social app. */
export type Post = {
  __typename?: 'Post';
  /** Comments with there ChainId */
  comments: Array<Comment>;
  /** The post's image_url(optional). */
  imageUrl?: Maybe<Scalars['String']['output']>;
  /** The key identifying the post, including the timestamp, author and index. */
  key: Key;
  /** The total number of likes */
  likes: Scalars['Int']['output'];
  /** The post's text content. */
  text: Scalars['String']['output'];
};

/** The application state. */
export type Social = {
  __typename?: 'Social';
  /** Our posts. */
  ownPosts: LogView_OwnPost_D65e13e4;
  /** Posts we received from authors we subscribed to. */
  receivedPosts: CustomMapView_Key_Post;
};

export type ReceivedPostsQueryVariables = Exact<{ [key: string]: never; }>;


export type ReceivedPostsQuery = { __typename?: 'Social', receivedPosts: { __typename?: 'CustomMapView_Key_Post', entries: Array<{ __typename?: 'Entry_Key_Post_1d4d0dae', value?: { __typename?: 'Post', text: string, imageUrl?: string | null, likes: number, key: { __typename?: 'Key', timestamp: any, author: any, index: number }, comments: Array<{ __typename?: 'Comment', text: string, chainId: any }> } | null }> } };

export type CreatePostMutationVariables = Exact<{
  message: Scalars['String']['input'];
  image?: InputMaybe<Scalars['String']['input']>;
}>;


export type CreatePostMutation = { __typename?: 'OperationMutationRoot', post: Array<number> };

export type LikePostMutationVariables = Exact<{
  key: KeyInput;
}>;


export type LikePostMutation = { __typename?: 'OperationMutationRoot', like: Array<number> };

export type CommentOnPostMutationVariables = Exact<{
  key: KeyInput;
  text: Scalars['String']['input'];
}>;


export type CommentOnPostMutation = { __typename?: 'OperationMutationRoot', comment: Array<number> };


export const ReceivedPostsDocument = {"kind":"Document","definitions":[{"kind":"OperationDefinition","operation":"query","name":{"kind":"Name","value":"ReceivedPosts"},"selectionSet":{"kind":"SelectionSet","selections":[{"kind":"Field","name":{"kind":"Name","value":"receivedPosts"},"selectionSet":{"kind":"SelectionSet","selections":[{"kind":"Field","name":{"kind":"Name","value":"entries"},"selectionSet":{"kind":"SelectionSet","selections":[{"kind":"Field","name":{"kind":"Name","value":"value"},"selectionSet":{"kind":"SelectionSet","selections":[{"kind":"Field","name":{"kind":"Name","value":"key"},"selectionSet":{"kind":"SelectionSet","selections":[{"kind":"Field","name":{"kind":"Name","value":"timestamp"}},{"kind":"Field","name":{"kind":"Name","value":"author"}},{"kind":"Field","name":{"kind":"Name","value":"index"}}]}},{"kind":"Field","name":{"kind":"Name","value":"text"}},{"kind":"Field","name":{"kind":"Name","value":"imageUrl"}},{"kind":"Field","name":{"kind":"Name","value":"comments"},"selectionSet":{"kind":"SelectionSet","selections":[{"kind":"Field","name":{"kind":"Name","value":"text"}},{"kind":"Field","name":{"kind":"Name","value":"chainId"}}]}},{"kind":"Field","name":{"kind":"Name","value":"likes"}}]}}]}}]}}]}}]} as unknown as DocumentNode<ReceivedPostsQuery, ReceivedPostsQueryVariables>;
export const CreatePostDocument = {"kind":"Document","definitions":[{"kind":"OperationDefinition","operation":"mutation","name":{"kind":"Name","value":"createPost"},"variableDefinitions":[{"kind":"VariableDefinition","variable":{"kind":"Variable","name":{"kind":"Name","value":"message"}},"type":{"kind":"NonNullType","type":{"kind":"NamedType","name":{"kind":"Name","value":"String"}}}},{"kind":"VariableDefinition","variable":{"kind":"Variable","name":{"kind":"Name","value":"image"}},"type":{"kind":"NamedType","name":{"kind":"Name","value":"String"}}}],"selectionSet":{"kind":"SelectionSet","selections":[{"kind":"Field","name":{"kind":"Name","value":"post"},"arguments":[{"kind":"Argument","name":{"kind":"Name","value":"text"},"value":{"kind":"Variable","name":{"kind":"Name","value":"message"}}},{"kind":"Argument","name":{"kind":"Name","value":"imageUrl"},"value":{"kind":"Variable","name":{"kind":"Name","value":"image"}}}]}]}}]} as unknown as DocumentNode<CreatePostMutation, CreatePostMutationVariables>;
export const LikePostDocument = {"kind":"Document","definitions":[{"kind":"OperationDefinition","operation":"mutation","name":{"kind":"Name","value":"likePost"},"variableDefinitions":[{"kind":"VariableDefinition","variable":{"kind":"Variable","name":{"kind":"Name","value":"key"}},"type":{"kind":"NonNullType","type":{"kind":"NamedType","name":{"kind":"Name","value":"KeyInput"}}}}],"selectionSet":{"kind":"SelectionSet","selections":[{"kind":"Field","name":{"kind":"Name","value":"like"},"arguments":[{"kind":"Argument","name":{"kind":"Name","value":"key"},"value":{"kind":"Variable","name":{"kind":"Name","value":"key"}}}]}]}}]} as unknown as DocumentNode<LikePostMutation, LikePostMutationVariables>;
export const CommentOnPostDocument = {"kind":"Document","definitions":[{"kind":"OperationDefinition","operation":"mutation","name":{"kind":"Name","value":"CommentOnPost"},"variableDefinitions":[{"kind":"VariableDefinition","variable":{"kind":"Variable","name":{"kind":"Name","value":"key"}},"type":{"kind":"NonNullType","type":{"kind":"NamedType","name":{"kind":"Name","value":"KeyInput"}}}},{"kind":"VariableDefinition","variable":{"kind":"Variable","name":{"kind":"Name","value":"text"}},"type":{"kind":"NonNullType","type":{"kind":"NamedType","name":{"kind":"Name","value":"String"}}}}],"selectionSet":{"kind":"SelectionSet","selections":[{"kind":"Field","name":{"kind":"Name","value":"comment"},"arguments":[{"kind":"Argument","name":{"kind":"Name","value":"key"},"value":{"kind":"Variable","name":{"kind":"Name","value":"key"}}},{"kind":"Argument","name":{"kind":"Name","value":"comment"},"value":{"kind":"Variable","name":{"kind":"Name","value":"text"}}}]}]}}]} as unknown as DocumentNode<CommentOnPostMutation, CommentOnPostMutationVariables>;