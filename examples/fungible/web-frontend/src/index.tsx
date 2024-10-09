import React from 'react';
import ReactDOM from 'react-dom/client';
import {
  BrowserRouter,
  Route,
  Routes,
  useParams,
  useSearchParams,
} from 'react-router-dom'; 
import GraphQLProvider from './GraphQLProvider';
import App from './App';
import './index.css';

const root = ReactDOM.createRoot(
  document.getElementById('root') as HTMLElement
);
root.render(
  <React.StrictMode>
     <BrowserRouter>
    <Routes>
      <Route path=":id" element={<GraphQLApp />} />
    </Routes>
  </BrowserRouter>
  </React.StrictMode>
);


function GraphQLApp() {
  const { id } = useParams();
  const [searchParams] = useSearchParams();
  let app = searchParams.get("app");
  let owner = searchParams.get("owner");
  let port = searchParams.get("port");
  if (app == null) {
    throw Error("missing app query param");
  }
  if (owner == null) {
    throw Error("missing owner query param");
  }
  if (port == null) {
    port = "8080";
  }
  return (
    <GraphQLProvider chainId={id} applicationId={app} port={port}>
      <App chainId={id || ''} owner={owner} />
    </GraphQLProvider>
  );
}
