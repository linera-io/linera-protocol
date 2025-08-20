import React from 'react';
import LogoImage from '../assets/images/logo.png';

function Header() {
  return (
    <header className="w-full h-20 border-b flex justify-center items-center border-slate-900/10">
    <div className="container w-full h-full flex justify-between items-center px-4">
      <img className="w-16 h-auto" alt="logo" src={LogoImage} />
      <h1 className="text-3xl text-[#f52a05] font-extrabold">Linera Pay</h1>
      <div></div>
    </div>
  </header>
  )
}

export default Header;
