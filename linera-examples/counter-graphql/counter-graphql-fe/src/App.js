import {
  useMutation,
  useSubscription,
  gql,
  useLazyQuery,
} from "@apollo/client";
import logo from "./logo.png";
import "./App.css";
import React from "react";

const GET_COUNTER_VALUE = gql`
  query {
    value
  }
`;

const INCREMENT_COUNTER = gql`
  mutation {
    executeOperation(operation: { increment: 1 })
  }
`;

const NOTIFICATION_SUBSCRIPTION = gql`
  subscription {
    notifications(
      chainIds: [
        "7817752ff06b8266d77df8febf5c4b524cec096bd83dc54f989074fb94f833737ae984f32be2cee1dfab766fe2d0c726503c4d97117eb59023e9cc65a8ecd1f7"
      ]
    )
  }
`;

function App() {
  let [valueQuery, { data, called }] = useLazyQuery(GET_COUNTER_VALUE, {
    fetchPolicy: "network-only",
  });
  useSubscription(NOTIFICATION_SUBSCRIPTION, {
    onData: () => valueQuery(),
  });
  if (!called) {
    void valueQuery();
  }
  return (
    <div className="App">
      <header className="App-header">
        <img src={logo} className="App-logo" alt="logo" />
        <p>Counter Value: {data ? data.value : "Loading..."}</p>
        <Increment />
      </header>
    </div>
  );
}

function Increment() {
  const [mutateFunction] = useMutation(INCREMENT_COUNTER);
  return (
    <button className="increment" onClick={() => mutateFunction()}>
      Increment
    </button>
  );
}

export default App;
