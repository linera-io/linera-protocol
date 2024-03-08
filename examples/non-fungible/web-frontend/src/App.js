import { useState } from "react";
import React from "react";
import { gql, useMutation, useLazyQuery, useSubscription } from "@apollo/client";
import { Card, Container, CardContent, Typography, Button, Dialog, DialogTitle, DialogContent, TextField, DialogActions, Paper, TableContainer, Table, TableHead, TableRow, TableCell, TableBody, CircularProgress } from '@mui/material';

const GET_OWNED_NFTS = gql`
  query OwnedNfts($owner: AccountOwner) {
    ownedNfts(owner: $owner)
  }
`;

const MINT_NFT = gql`
  mutation Mint($name: AccountOwner, $payload: Int) {
    mint(name: $name, payload: $payload)
  }
`;

const TRANSFER_NFT = gql`
  mutation Transfer($sourceOwner: AccountOwner, $tokenId: String, $targetAccount: Account) {
    transfer(sourceOwner: $sourceOwner, tokenId: $tokenId, targetAccount: $targetAccount)
  }
`;

const NOTIFICATION_SUBSCRIPTION = gql`
  subscription Notifications($chainId: ID!) {
    notifications(chainId: $chainId)
  }
`;

function App({ chainId, owner }) {
  // Error
  const [transferError, setTransferError] = useState("");
  const [mintError, setMintError] = useState("");

  // Dialog control
  const [isMintOpen, setMintOpen] = useState(false);
  const [isTransferOpen, setTransferOpen] = useState(false);

  // Transfer dialog
  const [tokenID, setTokenID] = useState('');
  const [targetChainID, setTargetChainID] = useState('');
  const [targetOwner, setTargetOwner] = useState('');

  // Mint dialog
  const [name, setName] = useState('');
  const [payload, setPayload] = useState(0);

  let [
    getOwnedNfts,
    { data: ownedNftsData, loading: ownedNftsLoading, error: ownedNftsError },
  ] = useLazyQuery(GET_OWNED_NFTS, {
    fetchPolicy: "network-only",
    variables: { owner: `User:${owner}` },
  });

  const [transferNft, { loading: transferLoading }] = useMutation(TRANSFER_NFT, {
    onError: (error) => setTransferError("Transfer Error: " + error.message),
    onCompleted: () => {
      getOwnedNfts(); // Refresh owned NFTs list
    },
  });

  const [mintNft, { loading: mintLoading }] = useMutation(MINT_NFT, {
    onError: (error) => setMintError("Mint Error: " + error.message),
    onCompleted: () => {
      getOwnedNfts(); // Refresh owned NFTs list
    },
  });

  if (!ownedNftsLoading) {
    void getOwnedNfts();
  }

  useSubscription(NOTIFICATION_SUBSCRIPTION, {
    variables: { chainId: chainId },
    onData: () => getOwnedNfts(), // Refresh on new notifications
  });

  const handleMintOpen = () => setMintOpen(true);
  const handleMintClose = () => setMintOpen(false);

  const handleTransferOpen = () => setTransferOpen(true);
  const handleTransferClose = () => setTransferOpen(false);

  const resetMintDialog = () => {
    setName("");
    setPayload("");
  };

  // Placeholder for form submission logic
  const handleMintSubmit = () => {
    mintNft({
      variables: {
        name: name,
        payload: Number(payload),
      },
    }).then(r => console.log("NFT minted: " + JSON.stringify(r, null, 2)));

    handleMintClose();
    resetMintDialog();
  };

  const resetTransferDialog = () => {
    setTokenID("");
    setTargetChainID("");
    setTargetOwner("");
  };

  const handleTransferSubmit = () => {
    transferNft({
      variables: {
        sourceOwner: `User:${owner}`,
        tokenId: tokenID,
        targetAccount: {
          chainId: targetChainID,
          owner: `User:${targetOwner}`,
        }
      },
    }).then(r => console.log("NFT transferred: " + JSON.stringify(r, null, 2)));

    handleTransferClose();
    resetTransferDialog();
  };

  return (
    <Container sx={{ mt: 4, overflowX: 'auto' }}>
      <Card sx={{ minWidth: 'auto', width: '100%', mx: 'auto', my: 2 }}>
        <CardContent error={ownedNftsError}>
          <Typography style={{ fontWeight: 'bold' }} variant="h5" component="div" gutterBottom>Linera NFT</Typography>
          <Typography style={{ fontWeight: 'bold' }} sx={{ mb: 1.5 }} color="text.secondary">Account: </Typography>
          <Typography sx={{ mb: 1.5 }} color="text.secondary">{owner}</Typography>
          <Typography style={{ fontWeight: 'bold' }} sx={{ mb: 1.5 }} color="text.secondary">Chain: </Typography>
          <Typography sx={{ mb: 1.5 }} color="text.secondary">{chainId}</Typography>

          <Typography style={{ fontWeight: 'bold' }} variant="h6" gutterBottom>Your Owned NFTs:</Typography>
          {ownedNftsData ? (
            Object.keys(ownedNftsData.ownedNfts).length === 0 ? (
              <Typography variant="body1" gutterBottom>No owned NFTs</Typography>
            ) : (
              <TableContainer component={Paper}>
                <Table sx={{ minWidth: 'auto' }} aria-label="simple table">
                  <TableHead>
                    <TableRow>
                      <TableCell sx={{ borderRight: '1px solid rgba(224, 224, 224, 1)' }}>Token Id</TableCell>
                      <TableCell sx={{ borderRight: '1px solid rgba(224, 224, 224, 1)' }} align="center">Name</TableCell>
                      <TableCell sx={{ borderRight: '1px solid rgba(224, 224, 224, 1)' }} align="center">Minter</TableCell>
                      <TableCell align="center">Payload</TableCell>
                    </TableRow>
                  </TableHead>
                  <TableBody>
                    {Object.entries(ownedNftsData.ownedNfts).map(([token_id, nft]) => (
                      <TableRow
                        key={token_id}
                        sx={{ '&:last-child td, &:last-child th': { border: 0 } }}
                      >
                        <TableCell sx={{ borderRight: '1px solid rgba(224, 224, 224, 1)' }} component="th" scope="row">
                          {token_id}
                        </TableCell>
                        <TableCell sx={{ borderRight: '1px solid rgba(224, 224, 224, 1)' }} align="center">{nft.name}</TableCell>
                        <TableCell sx={{ borderRight: '1px solid rgba(224, 224, 224, 1)' }} align="center">{nft.minter}</TableCell>
                        <TableCell align="center">{nft.payload}</TableCell>
                      </TableRow>
                    ))}
                  </TableBody>
                </Table>
              </TableContainer>
            )
          ) : (
            <CircularProgress size={50} color="primary" />
          )}

          <div style={{ display: 'flex', justifyContent: 'space-around', marginTop: '20px' }}>
            <Button variant="contained" onClick={handleMintOpen}>Mint</Button>
            <Button variant="contained" onClick={handleTransferOpen}>Transfer</Button>
          </div>
        </CardContent>

        <Dialog open={isMintOpen} onClose={handleMintClose} error={mintError}>
          <DialogTitle>Mint NFT</DialogTitle>
          <DialogContent>
            <TextField autoFocus margin="dense" id="name" label="Name" type="text" fullWidth variant="standard"
              value={name} onChange={(val) => setName(val.target.value)}
            />
            <TextField autoFocus margin="dense" id="payload" label="Payload" type="text" fullWidth variant="standard"
              value={payload} onChange={(val) => setPayload(val.target.value)}
            />
          </DialogContent>
          <DialogActions>
            <Button onClick={handleMintClose}>Cancel</Button>
            <Button onClick={handleMintSubmit}>Submit</Button>
            {mintLoading && <CircularProgress size={24} color="primary" />}
          </DialogActions>
        </Dialog>

        <Dialog open={isTransferOpen} onClose={handleTransferClose} error={transferError}>
          <DialogTitle>Transfer NFT</DialogTitle>
          <DialogContent>
            <TextField autoFocus margin="dense" id="token_id" label="Token Id" type="text" fullWidth variant="standard"
              value={tokenID} onChange={(val) => setTokenID(val.target.value)}
            />
            <TextField autoFocus margin="dense" id="target_chain_id" label="Target Chain Id" type="text" fullWidth variant="standard"
              value={targetChainID} onChange={(val) => setTargetChainID(val.target.value)}
            />
            <TextField autoFocus margin="dense" id="target_owner" label="Target Owner" type="text" fullWidth variant="standard"
              value={targetOwner} onChange={(val) => setTargetOwner(val.target.value)}
            />
          </DialogContent>
          <DialogActions>
            <Button onClick={handleTransferClose}>Cancel</Button>
            <Button onClick={handleTransferSubmit}>Submit</Button>
            {transferLoading && <CircularProgress size={24} color="primary" />}
          </DialogActions>
        </Dialog>
      </Card>
    </Container >
  );
}

export default App;
