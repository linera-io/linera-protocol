import React from 'react'
import UploadImage from '../assets/uploadimage.png'
import { gql, useMutation } from '@apollo/client'
import { CreatePostMutationVariables } from '../__generated__/graphql'

function UserInfo({ chainId }: { chainId: string }) {
  return (
    <div className="flex items-center mb-3 p-1 w-full">
      <img
        className="w-10 h-10 rounded-full"
        src="https://img.icons8.com/fluency/48/test-account--v1.png"
        alt="User"
      />
      <div className="ml-2 w-full">
        <div className="w-fit font-semibold text-xs break-all whitespace-normal">
          {chainId}
        </div>
        <div className="text-xs text-gray-500">Software Engineer</div>
      </div>
    </div>
  )
}

export default function NewPost({ chainId }: { chainId: string }) {
  const [showImgInput, setShowImgInput] = React.useState<boolean>(false)
  const [post, setPost] = React.useState<CreatePostMutationVariables>({
    message: '',
    image: '',
  })
  const [createPost] = useMutation<CreatePostMutationVariables>(gql`
    mutation createPost($message: String!, $image: String) {
      post(text: $message, imageUrl: $image)
    }
  `)

  async function handlePost() {
    await createPost({
      variables: {
        message: post.message,
        image: post.image,
      },
    })

    setPost({ message: '', image: '' })
  }
  return (
    <div className="border p-1 bg-slate-100 rounded-xl">
      <div className="w-[500px]">
        <UserInfo chainId={chainId} />
        <div className="flex flex-col justify-between h-full">
          <textarea
            value={post.message}
            onChange={(e) => setPost({ ...post, message: e.target.value })}
            placeholder="Write your post..."
            className="bg-transparent outline-none p-2 max-w-[400px] resize-none h-10 mb-5 rounded-md"
          />
          {post.image && (
            <img
              src={post.image}
              alt="post"
              className="w-full object-cover rounded-md"
            />
          )}
          <div className="flex justify-between mt-2 gap-2">
            {!showImgInput && (
              <button
                onClick={() => setShowImgInput(!showImgInput)}
                className="p-2 w-10 h-10 rounded-md"
              >
                <img src={UploadImage} alt="post" />
              </button>
            )}
            {showImgInput && (
              <input
                type="text"
                value={post.image ?? ''}
                placeholder="Add img url..."
                className="mb-0 px-4 py-1 outline-none rounded-xl w-full"
                onChange={(e) => {
                  setPost({ ...post, image: e.target.value })
                }}
              />
            )}
            {post.image && (
              <button
                className="px-4 py-1 text-white text-sm bg-[#fe2b00] rounded-full"
                onClick={() => {
                  setShowImgInput(!showImgInput)
                  setPost({ ...post, image: '' })
                }}
              >
                Clear
              </button>
            )}
            <button
              onClick={() => handlePost()}
              className="bg-[#fe2b00] text-sm text-white px-6 rounded-full"
            >
              Post
            </button>
          </div>
        </div>
      </div>
    </div>
  )
}
