/* eslint-disable */
import * as types from './graphql';
import { TypedDocumentNode as DocumentNode } from '@graphql-typed-document-node/core';

/**
 * Map of all GraphQL operations in the project.
 *
 * This map has several performance disadvantages:
 * 1. It is not tree-shakeable, so it will include all operations in the project.
 * 2. It is not minifiable, so the string of a GraphQL query will be multiple times inside the bundle.
 * 3. It does not support dead code elimination, so it will add unused operations.
 *
 * Therefore it is highly recommended to use the babel or swc plugin for production.
 */
const documents = {
    "\n  query ReceivedPosts {\n    receivedPosts {\n      entries {\n        value {\n          key {\n            timestamp\n            author\n            index\n          }\n          text\n          imageUrl\n          comments {\n            text\n            chainId\n          }\n          likes\n        }\n      }\n    }\n  }\n": types.ReceivedPostsDocument,
    "\n    mutation createPost($message: String!, $image: String) {\n      post(text: $message, imageUrl: $image)\n    }\n  ": types.CreatePostDocument,
    "\n    mutation likePost($key: KeyInput!) {\n      like(key: $key)\n    }\n  ": types.LikePostDocument,
    "\n    mutation CommentOnPost($key: KeyInput!, $text: String!) {\n      comment(key: $key, comment: $text)\n    }\n  ": types.CommentOnPostDocument,
};

/**
 * The gql function is used to parse GraphQL queries into a document that can be used by GraphQL clients.
 *
 *
 * @example
 * ```ts
 * const query = gql(`query GetUser($id: ID!) { user(id: $id) { name } }`);
 * ```
 *
 * The query argument is unknown!
 * Please regenerate the types.
 */
export function gql(source: string): unknown;

/**
 * The gql function is used to parse GraphQL queries into a document that can be used by GraphQL clients.
 */
export function gql(source: "\n  query ReceivedPosts {\n    receivedPosts {\n      entries {\n        value {\n          key {\n            timestamp\n            author\n            index\n          }\n          text\n          imageUrl\n          comments {\n            text\n            chainId\n          }\n          likes\n        }\n      }\n    }\n  }\n"): (typeof documents)["\n  query ReceivedPosts {\n    receivedPosts {\n      entries {\n        value {\n          key {\n            timestamp\n            author\n            index\n          }\n          text\n          imageUrl\n          comments {\n            text\n            chainId\n          }\n          likes\n        }\n      }\n    }\n  }\n"];
/**
 * The gql function is used to parse GraphQL queries into a document that can be used by GraphQL clients.
 */
export function gql(source: "\n    mutation createPost($message: String!, $image: String) {\n      post(text: $message, imageUrl: $image)\n    }\n  "): (typeof documents)["\n    mutation createPost($message: String!, $image: String) {\n      post(text: $message, imageUrl: $image)\n    }\n  "];
/**
 * The gql function is used to parse GraphQL queries into a document that can be used by GraphQL clients.
 */
export function gql(source: "\n    mutation likePost($key: KeyInput!) {\n      like(key: $key)\n    }\n  "): (typeof documents)["\n    mutation likePost($key: KeyInput!) {\n      like(key: $key)\n    }\n  "];
/**
 * The gql function is used to parse GraphQL queries into a document that can be used by GraphQL clients.
 */
export function gql(source: "\n    mutation CommentOnPost($key: KeyInput!, $text: String!) {\n      comment(key: $key, comment: $text)\n    }\n  "): (typeof documents)["\n    mutation CommentOnPost($key: KeyInput!, $text: String!) {\n      comment(key: $key, comment: $text)\n    }\n  "];

export function gql(source: string) {
  return (documents as any)[source] ?? {};
}

export type DocumentType<TDocumentNode extends DocumentNode<any, any>> = TDocumentNode extends DocumentNode<  infer TType,  any>  ? TType  : never;