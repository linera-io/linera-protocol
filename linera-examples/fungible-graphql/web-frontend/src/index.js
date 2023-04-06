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
  const [searchParams, setSearchParams] = useSearchParams();
  let owner = searchParams.get("owner");
  let port = searchParams.get("port");
  if (owner == null) {
    throw Error("missing owner query param");
  }
  if (port == null) {
    port = 8080;
  }
  return (
    <GraphQLProvider applicationId={id} port={port}>
      <App owner={owner} />
    </GraphQLProvider>
  );
}
