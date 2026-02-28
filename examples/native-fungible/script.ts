/// <reference types="vite/client" />
/// <reference types="vite/types/importMeta.d.ts" />

import * as linera from "@linera/client";
import { ethers } from "ethers";

interface TransferFormControlsCollection extends HTMLFormControlsCollection {
    amount: HTMLInputElement;
    recipient: HTMLInputElement;
}

const gql = (query, variables = {}) => JSON.stringify({ query, variables });

function prependEntry(parent, variables) {
  const entry = parent.querySelector("template").content.cloneNode(true);
  for (const [name, value] of Object.entries(variables)) {
    const element = entry.querySelector(`.${name}`);
    if (element) element.textContent = value;
  }

  parent.insertBefore(entry, parent.firstChild);
}

async function updateBalance(application, owner, blockHash?) {
  const response = JSON.parse(
    await application.query(
      gql(
        `query { tickerSymbol, accounts { entry(key: "${owner}") { value } } }`,
      ),
      { blockHash },
    ),
  );
  document.querySelector("#ticker-symbol").textContent =
    response.data.tickerSymbol;
  document.querySelector("#balance").textContent = (+(
    response.data.accounts.entry?.value || 0
  )).toFixed(2);
}

async function transfer(application, donor, amount, recipient) {
  const errorContainer = document.querySelector("#errors");
  for (const element of errorContainer.children)
    if (!(element instanceof HTMLTemplateElement))
      errorContainer.removeChild(element);

  let errors = [];
  try {
    const match = recipient.match(
      /^(0x[0-9a-f]{40}|0x[0-9a-f]{64})@([0-9a-f]{64})$/i,
    );
    if (!match)
      throw new Error("Invalid recipient address: expected `owner@chain_id`");

    const query = gql(
      `mutation(
                  $donor: AccountOwner!,
                  $amount: Amount!,
                  $recipient: Account!,
              ) {
                  transfer(owner: $donor, amount: $amount, targetAccount: $recipient)
              }`,
      {
        donor,
        amount,
        recipient: { owner: match[1], chainId: match[2] },
      },
    );

    errors = JSON.parse(await application.query(query)).errors || [];
  } catch (e) {
    console.error(e);
    errors.push({ message: e.message });
  }

  for (const error of errors) prependEntry(errorContainer, error);
}
const transferForm = document.querySelector("form#transfer");
const submitButton: HTMLInputElement = transferForm.querySelector('input[type="submit"]');
submitButton.disabled = true;

transferForm.addEventListener("submit", (event) => {
  const elements = event.target.elements as TransferFormControlsCollection;
  event.preventDefault();
  transfer(
    application,
    owner,
    elements.amount.value,
    elements.recipient.value,
  );
});

document.querySelector("#copy-account").addEventListener("click", (event) => {
  event.preventDefault();
  navigator.clipboard.writeText(
    document.querySelector("#account-id").textContent,
  );
});

await linera.initialize();
const faucet = await new linera.Faucet(import.meta.env.LINERA_FAUCET_URL);
const mnemonic = ethers.Wallet.createRandom().mnemonic.phrase;
const signer = linera.signer.PrivateKey.fromMnemonic(mnemonic);
const wallet = await faucet.createWallet();
const owner = signer.address();
const chainId = await faucet.claimChain(wallet, owner);
const client = await new linera.Client(wallet, signer);
document.querySelector("#chain-id").innerText = chainId;
document.querySelector("#owner").innerText = owner;

const chain = await client.chain(chainId);
const application = await chain.application(
  import.meta.env.LINERA_APPLICATION_ID,
);

await updateBalance(application, owner, null);

chain.onNotification((notification) => {
  let newBlock = notification.reason.NewBlock;
  if (notification.reason.BlockExecuted)
    updateBalance(application, owner, notification.reason.BlockExecuted.hash);
  else if (newBlock) {
    prependEntry(document.querySelector("#logs"), newBlock);
    updateBalance(application, owner);
  }
});

submitButton.disabled = false;

document.querySelector("#account-id").textContent = `${owner}@${chainId}`;

await chain.transfer({
  recipient: {
    chain_id: chainId,
    owner,
  },
  amount: 10,
});
