import React from "react";
import ReactDOM from "react-dom/client";
import "./index.css";
import App from "./App";
import {
  BrowserRouter,
  Route,
  Routes,
  useParams,
  useSearchParams,
} from "react-router-dom";
import GraphQLProvider from "./GraphQLProvider";

const root = ReactDOM.createRoot(document.getElementById("root"));

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
  let port = searchParams.get("port");
  if (app == null) {
    throw Error("missing app query param");
  }
  if (port == null) {
    port = 8080;
  }
  return (
    <GraphQLProvider chainId={id} applicationId={app} port={port}>
      <App chainId={id} />
    </GraphQLProvider>
  );
}
