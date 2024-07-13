import React from 'react'
import Img1 from '../assets/wisewords.png'
import Img2 from '../assets/learn.png'
import Img3 from '../assets/meme1.jpg'
import Img4 from '../assets/meme2.jpg'

export default function RightSideMenu() {
  return (
    <div className="w-full p-5 h-fit bg-slate-100 border rounded-xl">
      <h1 className="font-bold text-2xl">Community Work</h1>
      <div className="flex flex-wrap gap-x-1 gap-y-2 mt-5">
        <span className="w-full h-full max-w-[180px] max-h-[180px]">
          <img src={Img1} alt="" />
        </span>
        <span className="w-full h-full max-w-[180px] max-h-[180px]">
          <img src={Img2} alt="" />
        </span>

        <span className="w-full h-full max-w-[180px] max-h-[180px]">
          <img src={Img3} alt="" />
        </span>

        <span className="w-full h-full max-w-[180px] max-h-[180px]">
          <img src={Img4} alt="" />
        </span>
      </div>
    </div>
  )
}
