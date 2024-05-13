import { useState } from "react";
import {
  gql,
  useMutation,
  useLazyQuery,
  useSubscription,
} from "@apollo/client";
import tw from "tailwind-styled-components";

const GET_BALANCE = gql`
  query Accounts($owner: AccountOwner) {
    accounts {
      entry(key: $owner) {
        value
      }
    }
  }
`;

const GET_TICKER_SYMBOL = gql`
  query TickerSymbol {
    tickerSymbol
  }
`;

const MAKE_PAYMENT = gql`
  mutation Transfer($owner: AccountOwner, $amount: Amount, $targetAccount: FungibleAccount) {
    transfer(owner: $owner, amount: $amount, targetAccount: $targetAccount)
  }
`;

const NOTIFICATION_SUBSCRIPTION = gql`
  subscription Notifications($chainId: ID!) {
    notifications(chainId: $chainId)
  }
`;

// Styled components
const Container = tw.div`
  max-w-2xl mx-auto my-8
`;

const Card = tw.div`
  bg-white rounded-lg shadow-md p-6 mb-6
`;

const Label = tw.label`
  block mb-2 text-gray-700 font-bold
`;

const Input = tw.input`
  w-full border rounded py-2 px-3 text-gray-700 leading-tight focus:outline-none focus:shadow-outline
`;

const Button = tw.button`
  bg-blue-500 hover:bg-blue-700 text-white font-bold py-2 px-4 rounded focus:outline-none focus:shadow-outline
`;

const ErrorMessage = tw.div`
  text-red-500 text-sm italic mt-2
`;

// App component
function App({ chainId, owner }) {
  const [recipient, setRecipient] = useState("");
  const [targetChain, setTargetChain] = useState("");
  const [amount, setAmount] = useState("");
  const [error, setError] = useState("");
  let [
    balanceQuery,
    { data: balanceData, called: balanceCalled, error: balanceError },
  ] = useLazyQuery(GET_BALANCE, {
    fetchPolicy: "network-only",
    variables: { owner: `User:${owner}` },
  });
  const [makePayment, { loading: paymentLoading }] = useMutation(MAKE_PAYMENT, {
    onError: (error) => setError("Error: " + error.networkError.result),
    onCompleted: () => {
      setRecipient("");
      setAmount("");
    },
  });

  if (!balanceCalled) {
    void balanceQuery();
  }

  useSubscription(NOTIFICATION_SUBSCRIPTION, {
    variables: { chainId: chainId },
    onData: () => balanceQuery(),
  });

  // Event handlers
  const handleRecipientChange = (event) => {
    setRecipient(event.target.value);
  };

  const handleAmountChange = (event) => {
    setAmount(event.target.value);
  };

  const handleTargetChainChange = (event) => {
    setTargetChain(event.target.value);
  };

  let [
    tickerSymbolQuery,
    { data: tickerSymbolData, called: tickerSymbolCalled, error: tickerSymbolError },
  ] = useLazyQuery(GET_TICKER_SYMBOL, { fetchPolicy: "network-only" });

  if (!tickerSymbolCalled) {
    void tickerSymbolQuery();
  }

  const handleSubmit = (event) => {
    event.preventDefault();
    makePayment({
      variables: {
        owner: `User:${owner}`,
        amount,
        targetAccount: {
          chainId: targetChain,
          owner: `User:${recipient}`,
        },
      },
    }).then(r => console.log("payment made: " + r));
  };

  // Render
  return (
    <Container>
      <h1 className="text-6xl font-normal leading-normal mt-0 mb-2">
        Linera Pay
      </h1>
      <Label htmlFor="recipient">Account: {owner}</Label>
      <Card>
        <h1 className="text-2xl font-bold mb-2">Your Balance</h1>
        {balanceData ? (
          <p className="text-3xl font-bold">
            {parseInt(balanceData.accounts.entry.value ?? '0').toLocaleString()}
            {tickerSymbolData && ' ' + tickerSymbolData.tickerSymbol}
          </p>
        ) : (
          <p>Loading...</p>
        )}
        {balanceError && (
          <ErrorMessage>Failed to pull balance. Re-trying...</ErrorMessage>
        )}
        {tickerSymbolError && (
          <ErrorMessage>Failed to read ticker symbol. Re-trying...</ErrorMessage>
        )}
      </Card>

      <Card>
        <h1 className="text-2xl font-bold mb-2">Make a Payment</h1>
        <form onSubmit={handleSubmit}>
          <div className="mb-4">
            <Label htmlFor="recipient">Recipient Account</Label>
            <Input
              type="text"
              id="targetChain"
              placeholder="Enter target chain"
              value={targetChain}
              onChange={handleTargetChainChange}
              required
            />
          </div>
          <div className="mb-4">
            <Input
              type="text"
              id="recipient"
              placeholder="Enter recipient account (Owner)"
              value={recipient}
              onChange={handleRecipientChange}
              required
            />
          </div>

          <div className="mb-4">
            <Label htmlFor="amount">Amount</Label>
            <Input
              type="number"
              id="amount"
              placeholder="Enter amount"
              value={amount}
              onChange={handleAmountChange}
              required
            />
          </div>

          <Button type="submit" disabled={paymentLoading}>
            {paymentLoading ? "Processing..." : "Send Payment"}
          </Button>
          {error && <ErrorMessage>{error}</ErrorMessage>}
        </form>
      </Card>
    </Container>
  );
}

export default App;
