import React from 'react'
import UploadImage from '../assets/uploadimage.png'
function UserInfo() {
  return (
    <div className="flex items-center mb-3 p-1">
      <img
        className="w-10 h-10 rounded-full"
        src="https://external-content.duckduckgo.com/iu/?u=https%3A%2F%2Ftse1.mm.bing.net%2Fth%3Fid%3DOIP.QV66R5EzC2y_EFSjHKcypAHaFj%26pid%3DApi&f=1&ipt=d78c19ed3c954411d8c4478fd819678df8185d674ef6642af3902080fa0296c2&ipo=images"
        alt="User"
      />
      <div className="ml-2">
        <div className="font-semibold">John Doe</div>
        <div className="text-xs text-gray-500">Software Engineer</div>
      </div>
    </div>
  )
}
export default function NewPost() {
  const [post, setPost] = React.useState({
    message: '',
    image: '',
  })
  const [showImgInput, setShowImgInput] = React.useState(false)
  return (
    <div className="border p-1 bg-slate-100 rounded-xl">
      <div className="w-[450px] h-full max-h-[600px]">
        <UserInfo />
        <div className="flex flex-col justify-between h-full">
          <input
            type="text"
            value={post.message}
            onChange={(e) => setPost({ ...post, message: e.target.value })}
            placeholder="Write your post..."
            className="bg-transparent outline-none p-2 rounded-md"
          />
          <div className="flex justify-between mt-2">
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
                value={post.image}
                placeholder="Add img url..."
                className="mb-0 px-4 py-1 outline-none"
                onChange={(e) => {
                  setPost({ ...post, image: e.target.value })
                }}
              />
            )}
            <button className="bg-blue-500 text-sm text-white px-6 rounded-full">
              Post
            </button>
          </div>
        </div>
      </div>
    </div>
  )
}
