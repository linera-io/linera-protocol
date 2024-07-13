import React from 'react'

function UserInfo() {
  return (
    <div className="flex items-center mb-5 p-1">
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
export default function PostCard({ post }) {
  const [showComment, setShowComment] = React.useState('')
  return (
    <div className="border p-1 bg-slate-100 rounded-xl">
      <div className="w-[450px] h-full max-h-[600px]">
        <UserInfo />
        <div className="my-2 p-1 break-words whitespace-normal">
          {post.value}
        </div>
        <div className="object-contain w-full h-fit">
          <img
            className="rounded-lg w-full h-full object-cover"
            src={post.imageUrl}
            alt="Post"
          />
        </div>
        <div className="flex justify-between p-2 mt-3 ">
          <div className="cursor-pointer flex items-center text-sm hover:scale-110 transition-all">
            <img
              width="28"
              height="28"
              src="https://img.icons8.com/fluency/48/love-circled.png"
              alt="love-circled"
            />{' '}
            Like
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
                className="w-8 h-8 rounded-full"
                src="https://external-content.duckduckgo.com/iu/?u=https%3A%2F%2Ftse1.mm.bing.net%2Fth%3Fid%3DOIP.QV66R5EzC2y_EFSjHKcypAHaFj%26pid%3DApi&f=1&ipt=d78c19ed3c954411d8c4478fd819678df8185d674ef6642af3902080fa0296c2&ipo=images"
                alt="User"
              />
              <input
                className="w-full px-3 py-2 rounded-xl ml-2"
                type="text"
                placeholder="Add a comment"
              />
              <button className="bg-[]">Send</button>
            </div>
          </div>
        )}
      </div>
    </div>
  )
}
