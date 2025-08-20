import React, { useMemo, useState } from "react";
import {
  gql,
  useMutation,
  useLazyQuery,
  useSubscription,
} from "@apollo/client";
import {
  AccountsQuery,
  TickerSymbolQuery,
  TransferMutation,
} from "./qql/graphql";
import Header from "./components/Header";
import ErrorMessage from "./components/ErrorMessage";
import DialogSuccess from "./components/DialogSuccess";
import { IconLoader } from "./assets/icons/Loader";

const GET_BALANCE = gql`
  query Accounts($owner: AccountOwner!) {
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
  mutation Transfer(
    $owner: AccountOwner!
    $amount: Amount!
    $targetAccount: FungibleAccount!
  ) {
    transfer(owner: $owner, amount: $amount, targetAccount: $targetAccount)
  }
`;

const NOTIFICATION_SUBSCRIPTION = gql`
  subscription Notifications($chainId: ChainId!) {
    notifications(chainId: $chainId)
  }
`;

type AppProps = {
  chainId: string;
  owner: string;
};

function App({ chainId, owner }: AppProps) {
  const [recipient, setRecipient] = useState("");
  const [targetChain, setTargetChain] = useState("");
  const [amount, setAmount] = useState("");
  const [error, setError] = useState("");
  const [openDialog, setOpenDialog] = useState(false);

  let [
    balanceQuery,
    { data: balanceData, called: balanceCalled, error: balanceError },
  ] = useLazyQuery<AccountsQuery>(GET_BALANCE, {
    onError: (error) => {
      console.error('failed to query balance', error);
    },
    fetchPolicy: "network-only",
    variables: { owner },
  });

  useSubscription(NOTIFICATION_SUBSCRIPTION, {
    variables: { chainId: chainId },
    onError: () => {
      console.log('failed to subscribe to notifications');
    },
    onData: () => {
      console.log('querying balance');
      balanceQuery();
    },
  });

  if (!balanceCalled) {
    void balanceQuery();
  }

  let [
    tickerSymbolQuery,
    {
      data: tickerSymbolData,
      called: tickerSymbolCalled,
      error: tickerSymbolError,
    },
  ] = useLazyQuery<TickerSymbolQuery>(GET_TICKER_SYMBOL, {
    fetchPolicy: "network-only",
  });

  if (!tickerSymbolCalled) {
    void tickerSymbolQuery();
  }

  const [makePayment, { loading: paymentLoading }] =
    useMutation<TransferMutation>(MAKE_PAYMENT, {
      onError: (error) => setError(error?.networkError?.message || ""),
      onCompleted: () => {
        setTargetChain("");
        setRecipient("");
        setAmount("");
        setError("");
        setOpenDialog(true);
      },
    });

  const handleTargetChainChange = (event: {
    target: { value: React.SetStateAction<string> };
  }) => {
    setTargetChain(event.target.value);
  };

  const handleRecipientChange = (event: {
    target: { value: React.SetStateAction<string> };
  }) => {
    setRecipient(event.target.value);
  };

  const handleAmountChange = (event: {
    target: { value: React.SetStateAction<string> };
  }) => {
    setAmount(event.target.value);
  };

  const handleSubmit = (event: { preventDefault: () => void }) => {
    event.preventDefault();
    makePayment({
      variables: {
        owner: `${owner}`,
        amount,
        targetAccount: {
          chainId: targetChain,
          owner: `${recipient}`,
        },
      },
    }).then((r) => console.log("payment made", r));
  };

  const isMaxBalance = useMemo(() => {
    const balance = Number(balanceData?.accounts?.entry?.value) || 0;
    return Number(amount) > balance;
  }, [amount, balanceData]);

  return (
    <div className="w-full h-full">
      <Header />
      <div className="w-full h-full flex justify-center">
        <div className="w-full h-full container flex justify-center items-center flex-col py-10 lg:py-20">
          <div className="mb-10 px-4 py-6 lg:px-6 lg:py-10 w-full lg:max-w-[800px] flex items-start flex-col lg:rounded-md lg:border lg:border-slate-900/10 lg:shadow-md">
            <h2 className="text-2xl mb-4 font-bold text-center uppercase">
              Account information
            </h2>
            <div className="flex flex-col items-start gap-y-2 mb-4">
              <p className="font-bold text-lg lg:text-xl text-slate-900">
                Owner:
              </p>
              <p className="text-slate-600 text-lg lg:text-lg text-wrap break-all">
                {owner || ""}
              </p>
            </div>
            <div className="flex flex-col items-start justify-start gap-2">
              <p className="font-bold text-lg lg:text-xl text-slate-900">
                Balance:
              </p>
              <p className="text-slate-600 text-xl font-bold lg:text-3xl break-all">
                {!balanceError && (
                  <span>
                    {Number(balanceData?.accounts?.entry?.value) || 0}
                  </span>
                )}
                {!tickerSymbolError && (
                  <span className="ml-1 text-[#f52a05]">
                    {tickerSymbolData ? tickerSymbolData?.tickerSymbol : ""}
                  </span>
                )}
              </p>
              {balanceError && (
                <p className="text-yellow-700">
                  Failed to pull balance. Re-trying...
                </p>
              )}
              {tickerSymbolError && (
                <p className="text-yellow-700">
                  Failed to read ticker symbol. Re-trying...
                </p>
              )}
            </div>
          </div>
          <div className="min-w-[300px] w-full lg:max-w-[800px] px-4 py-6 lg:px-6 lg:py-10 lg:rounded-md lg:border lg:border-slate-900/10 lg:shadow-md">
            <h2 className="text-2xl font-bold text-center uppercase">
              Make a payment
            </h2>
            <div className="pt-6">
              <form
                onSubmit={handleSubmit}
                className="flex flex-col justify-center gap-y-6"
              >
                <div className="flex w-full flex-col gap-y-4">
                  <label className="font-bold" htmlFor="targetChain">
                    Target Chain:
                  </label>
                  <input
                    className="h-10 border border-slate-900/10 rounded-md px-4 py-2"
                    type="text"
                    id="targetChain"
                    placeholder="Enter target chain"
                    value={targetChain}
                    onChange={handleTargetChainChange}
                    required
                  />
                </div>
                <div className="flex w-full flex-col gap-y-4">
                  <label className="font-bold" htmlFor="recipient">
                    Recipient Account:
                  </label>
                  <input
                    className="h-10 border border-slate-900/10 rounded-md px-4 py-2"
                    type="text"
                    id="recipient"
                    placeholder="Enter recipient account (Owner)"
                    value={recipient}
                    onChange={handleRecipientChange}
                    required
                  />
                </div>
                <div className="flex w-full flex-col gap-y-4">
                  <label className="font-bold" htmlFor="amount">
                    Amount:
                  </label>
                  <input
                    className="h-10 border border-slate-900/10 rounded-md px-4 py-2"
                    type="number"
                    id="amount"
                    placeholder="Enter amount"
                    value={amount}
                    onChange={handleAmountChange}
                    required
                  />
                </div>
                {error && <ErrorMessage msg={error} />}

                {isMaxBalance && (
                  <ErrorMessage msg="Amount exceeds current balance" />
                )}
                <DialogSuccess
                  open={openDialog}
                  setOpenDialog={() => setOpenDialog(!openDialog)}
                />
                <button
                  disabled={paymentLoading || isMaxBalance}
                  className="w-full h-12 bg-[#f52a05] hover:bg-red-600 text-white rounded-md disabled:bg-red-400"
                  type="submit"
                >
                  {paymentLoading ? (
                    <div className="flex w-full h-full justify-center items-center gap-x-4">
                      <IconLoader />
                      Processing...
                    </div>
                  ) : (
                    "Send Payment"
                  )}
                </button>
              </form>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}

export default App;
