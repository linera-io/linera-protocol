import {
  useMutation,
  useSubscription,
  gql,
  useLazyQuery,
} from "@apollo/client";
import "./App.css";
import React from "react";

const GET_COUNTER_VALUE = gql`
  query {
    value
  }
`;

const INCREMENT_COUNTER = gql`
  mutation {
    increment(value: 1)
  }
`;

const NOTIFICATION_SUBSCRIPTION = gql`
  subscription Notifications($chainId: ID!) {
    notifications(chainId: $chainId)
  }
`;

function App({ chainId }) {
  let [valueQuery, { data, called }] = useLazyQuery(GET_COUNTER_VALUE, {
    fetchPolicy: "network-only",
  });
  useSubscription(NOTIFICATION_SUBSCRIPTION, {
    variables: { chainId: chainId },
    onData: () => valueQuery()
  });
  if (!called) {
    void valueQuery();
  }
  return (
    <div className="App">
      <header className="App-header">
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
