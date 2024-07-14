import React from 'react'
import Navbar from './components/Navbar'
import NewPost from './components/NewPost'
import PostCard from './components/PostCard'
import LeftSideMenu from './components/LeftSideMenu'
import RightSideMenu from './components/RightSideMenu'
import { gql, useSubscription, useLazyQuery } from '@apollo/client'

const NOTIFICATION_SUBSCRIPTION = gql`
  subscription Notifications($chainId: ID!) {
    notifications(chainId: $chainId)
  }
`
export default function App({ chainId, onwer }) {
  const [posts, setPosts] = React.useState([])
  const [receivedPosts, { called }] = useLazyQuery(
    gql`
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
    `,
    {
      onCompleted: (data) => {
        console.log('data', data)
        setPosts(data.receivedPosts.entries)
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
            posts?.map((post, index) => (
              <div key={index}>{<PostCard post={post.value} />}</div>
            ))}
        </div>
        <div className="h-fit w-[410px] fixed right-[50px]">
          <RightSideMenu />
        </div>
      </div>
    </div>
  )
}
