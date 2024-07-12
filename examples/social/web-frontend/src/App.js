import Navbar from './components/Navbar'
import NewPost from './components/NewPost'
import PostCard from './components/PostCard'

function App({ chainId, onwer }) {
  return (
    <div className="w-full flex items-center flex-col">
      <Navbar />
      <div className="mt-20 flex flex-col gap-3">
        <NewPost />
        <PostCard />
      </div>
    </div>
  )
}

export default App
