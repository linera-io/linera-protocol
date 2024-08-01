import Logo from '../assets/linera.png'
export default function Navbar({ chainId }: { chainId: string }) {
  return (
    <div className="w-full h-[80px] flex justify-center bg-transparent fixed backdrop-blur-xl z-10">
      <div className="w-full px-10 py-5 flex max-w-[1380px] justify-between">
        <div className="w-10 h-10 flex items-center gap-1">
          <img
            src={Logo}
            className="w-full h-full object-contain"
            alt="linera_logo"
          />
          <h1 className="text-xl font-bold font-sans">Casual</h1>
        </div>
        <div className="flex items-center gap-1">
          <h1 className="font-semibold text-sm">{chainId}</h1>
          <div className="w-12 h-12 rounded-full">
            <img
              width="full"
              height="full"
              src="https://img.icons8.com/fluency/48/test-account--v1.png"
              alt="test-account--v1"
            />
          </div>
        </div>
      </div>
    </div>
  )
}
