import React from 'react'
import Navbar from './components/Navbar'
import NewPost from './components/NewPost'
import PostCard from './components/PostCard'
import LeftSideMenu from './components/LeftSideMenu'
import RightSideMenu from './components/RightSideMenu'
import { gql, useSubscription, useLazyQuery } from '@apollo/client'
import { ReceivedPosts } from './types'
import { Post } from './__generated__/graphql'

export const RECEIVED_POSTS = gql`
  query {
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
          comment {
            text
            chainId
          }
          likes
        }
      }
    }
  }
`

const NOTIFICATION_SUBSCRIPTION = gql`
  subscription Notifications($chainId: String!) {
    notifications(chainId: $chainId)
  }
`
export default function App({ chainId }: { chainId: string }) {
  const [posts, setPosts] = React.useState<Post[]>([])
  const [receivedPosts, { called }] = useLazyQuery<ReceivedPosts>(
    RECEIVED_POSTS,
    {
      onCompleted: (data) => {
        console.log('Data loaded', data.value)
        if (data.value !== null && data.value !== undefined)
          setPosts([data.value])
      },
      fetchPolicy: 'network-only',
    }
  )
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
            posts?.map((post: Post, index) => (
              <div key={index}>{post && <PostCard post={post} />}</div>
            ))}
        </div>
        <div className="h-fit w-[410px] fixed right-[50px]">
          <RightSideMenu />
        </div>
      </div>
    </div>
  )
}
