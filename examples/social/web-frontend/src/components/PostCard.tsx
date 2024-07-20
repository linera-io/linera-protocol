import { gql, useMutation } from '@apollo/client'
import React from 'react'
import { convertMicrosToDateTime } from '../utils/time'
import {
  CommentOnPostMutationVariables,
  Key,
  LikePostMutationVariables,
  Post,
} from '../__generated__/graphql'

function UserInfo({ user }: { user: Key }) {
  // eslint-disable-next-line
  const { formattedDate, formattedTime } = convertMicrosToDateTime(
    user.timestamp
  )
  return (
    <div className="flex items-center mb-3 p-1 w-full">
      <img
        className="w-10 h-10 rounded-full"
        src="https://img.icons8.com/fluency/48/test-account--v1.png"
        alt="User"
      />
      <div className="ml-2 w-full">
        <div className="w-fit font-semibold text-xs break-all whitespace-normal">
          {user.author}
        </div>
        <div className="flex justify-between">
          <div className="text-xs text-gray-500">Software Engineer</div>
          <span className="text-sm font-semibold">
            {formattedTime}
            {/* {formattedDate} */}
          </span>
        </div>
      </div>
    </div>
  )
}

export default function PostCard({ post }: { post: Post }) {
  const [showComment, setShowComment] = React.useState<boolean>(false)
  const [comment, setComment] = React.useState<string>('')

  const [likeMutation] = useMutation<LikePostMutationVariables>(gql`
    mutation likePost($key: KeyInput!) {
      like(key: $key)
    }
  `)
  const [commentMutation] = useMutation<CommentOnPostMutationVariables>(gql`
    mutation CommentOnPost($key: KeyInput!, $text: String!) {
      comment(key: $key, comment: $text)
    }
  `)
  async function handleLike(key: Key) {
    await likeMutation({
      variables: {
        key: {
          timestamp: key.timestamp,
          author: key.author,
          index: key.index,
        },
      },
    })
  }

  async function handleComment(key: Key) {
    await commentMutation({
      variables: {
        key: {
          timestamp: key.timestamp,
          author: key.author,
          index: key.index,
        },
        text: comment,
      },
    })
    setComment('')
  }
  return (
    <div className="border p-1 bg-slate-100 rounded-xl">
      <div className="w-[500px] h-full max-h-fit">
        <UserInfo user={post.key} />
        <div className="mb-2 p-1 break-words whitespace-normal">
          {post.text}
        </div>
        {post.imageUrl && (
          <div className="object-contain w-full h-fit">
            <img
              className="rounded-lg w-full h-full object-cover"
              src={post.imageUrl}
              alt="Post"
            />
          </div>
        )}
        <div className="flex justify-between p-2 mt-3">
          <div
            onClick={() => handleLike(post.key)}
            className="cursor-pointer flex items-center text-sm hover:scale-110 transition-all"
          >
            <img
              width="28"
              height="28"
              src="https://img.icons8.com/fluency/48/love-circled.png"
              alt="love-circled"
            />
            {post.likes} Like
          </div>
          <div
            className="cursor-pointer text-sm flex hover:scale-110 transition-all items-center"
            onClick={() => setShowComment(!showComment)}
          >
            <img
              width="28"
              height="28"
              src="https://img.icons8.com/fluency/48/chat-message.png"
              alt="chat-message"
            />
            Comment
          </div>
        </div>
        {showComment && (
          <div className="p-2">
            <div className="flex items-center">
              <img
                src="https://img.icons8.com/fluency/48/test-account--v1.png"
                className="w-10 h-10 rounded-full"
                alt="User"
              />
              <input
                className="w-full px-3 py-2 rounded-xl ml-2"
                type="text"
                value={comment}
                onChange={(e) => setComment(e.target.value)}
                placeholder="Add a comment"
              />
              <button
                onClick={() => handleComment(post.key)}
                className="bg-[#fe2b00] rounded-full px-3 py-1 text-white text-sm ml-2"
              >
                Send
              </button>
            </div>
            {post.comments.length > 0 && (
              <div className="mt-4 w-full">
                <h1>{post.comments.length} Comments</h1>
                <div className="mt-2 text-sm font-semibold max-h-[200px] overflow-y-auto h-full">
                  {post.comments.map((comment, index) => (
                    <div key={index} className="flex items-center py-2">
                      <div className="w-full flex items-center gap-2">
                        <img
                          className="w-10 h-10 rounded-full self-start"
                          src="https://img.icons8.com/fluency/48/test-account--v1.png"
                          alt="User"
                        />
                        <div className="w-full max-w-fit">
                          <h1 className="break-all font-semibold text-xs text-slate-800 whitespace-normal w-fit">
                            {comment.chainId}
                          </h1>
                          <h1 className="mt-2 font-normal">{comment.text}</h1>
                        </div>
                      </div>
                    </div>
                  ))}
                </div>
              </div>
            )}
          </div>
        )}
      </div>
    </div>
  )
}
