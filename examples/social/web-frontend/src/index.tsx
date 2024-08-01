import ReactDOM from 'react-dom/client'
import './index.css'
import {
  BrowserRouter,
  Route,
  Routes,
  useParams,
  useSearchParams,
} from 'react-router-dom'
import GraphQLProvider from './GraphQLProvider'
import App from './App'

const root = ReactDOM.createRoot(document.getElementById('root')!)

root.render(
  <BrowserRouter>
    <Routes>
      <Route path=":id" element={<GraphQLApp />} />
    </Routes>
  </BrowserRouter>
)

function GraphQLApp() {
  const { id } = useParams()
  const [searchParams] = useSearchParams()
  const app = searchParams.get('app')
  const owner = searchParams.get('owner')
  let port = parseInt(searchParams.get('port') || '', 10)

  if (!app) {
    // Consider a more graceful way to handle missing parameters, like showing an error message in the UI
    throw Error('Missing app query param')
  }

  if (!owner) {
    // Consider a more graceful way to handle missing parameters, like showing an error message in the UI
    throw Error('Missing owner query param')
  }

  // Ensure port is a number. If not, default to 8000.
  if (isNaN(port)) {
    port = 8000
  }

  return (
    <GraphQLProvider chainId={id} applicationId={app} port={port}>
      <App chainId={id ?? ''} />
      {/* chainId is a required prop, you can pass owner prop here */}
    </GraphQLProvider>
  )
}
