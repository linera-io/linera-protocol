import React from 'react'
import Navbar from './components/Navbar'
import NewPost from './components/NewPost'
import PostCard from './components/PostCard'
import LeftSideMenu from './components/LeftSideMenu'
import { gql, useSubscription, useLazyQuery } from '@apollo/client'
import { Post, Social } from './__generated__/graphql'

interface ReceivedPosts {
  value: Post
}

export const RECEIVED_POSTS = gql`
  query ReceivedPosts {
    receivedPosts {
      entries {
        value {
          key {
            timestamp
            author
            index
          }
          text
          imageUrl
          comments {
            text
            chainId
          }
          likes
        }
      }
    }
  }
`

// A tag for GraphQL queries that are destined for the node service
// rather than the application.  This exists to prevent
// `graphql-codegen` getting confused when generating TypeScript
// bindings for the application GraphQL queries.
const nodeGql = gql;

const NOTIFICATION_SUBSCRIPTION = nodeGql`
  subscription Notifications($chainId: String!) {
    notifications(chainId: $chainId)
  }
`
export default function App({ chainId }: { chainId: string }) {
  const [posts, setPosts] = React.useState<ReceivedPosts[]>([])
  const [receivedPosts, { called }] = useLazyQuery<Social>(RECEIVED_POSTS, {
    onCompleted: (data: Social) => {
      setPosts(data.receivedPosts.entries as ReceivedPosts[])
    },
    fetchPolicy: 'network-only',
  })
  useSubscription(NOTIFICATION_SUBSCRIPTION, {
    variables: { chainId },
    onData: () => receivedPosts(),
  })
  if (!called) {
    void receivedPosts()
  }

  return (
    <div className="w-full flex items-center flex-col">
      <Navbar chainId={chainId} />
      <div className="mt-20 p-2 w-full max-w-[1320px] relative min-h-screen flex gap-3 justify-center mb-20">
        <div className="h-full fixed left-[160px]">
          <LeftSideMenu />
        </div>
        <div className="flex flex-col gap-3">
          <NewPost chainId={chainId} />
          {posts &&
            posts?.map((post: ReceivedPosts, index) => (
              <div key={index}>
                {post.value && <PostCard post={post.value} />}
              </div>
            ))}
        </div>
      </div>
    </div>
  )
}
