import { useState } from "react";
import React from "react";
import {gql, useMutation, useLazyQuery, useSubscription} from "@apollo/client";
import { Card, Typography, Button, Table, Layout, Modal, Form, Input, Space, Alert, Descriptions} from 'antd';
import Loader from './Loader.js'

const { Title } = Typography;

const GET_OWNED_NFTS = gql`
  query OwnedNfts($owner: AccountOwner!) {
    ownedNfts(owner: $owner)
  }
`;

const MINT_NFT = gql`
  mutation Mint($minter: AccountOwner!, $prompt: String!) {
    mint(minter: $minter, prompt: $prompt)
  }
`;

const TRANSFER_NFT = gql`
  mutation Transfer($sourceOwner: AccountOwner!, $tokenId: String!, $targetAccount: FungibleAccount!) {
    transfer(sourceOwner: $sourceOwner, tokenId: $tokenId, targetAccount: $targetAccount)
  }
`;

const NOTIFICATION_SUBSCRIPTION = gql`
  subscription Notifications($chainId: ID!) {
    notifications(chainId: $chainId)
  }
`;

const PROMPT = gql`
query Prompt($prompt: String) {
  prompt(prompt: $prompt)
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
  const [transferForm] = Form.useForm();

  // Mint dialog
  const [mintForm] = Form.useForm();
  const [promptPreview, setPromptPreview] = useState("");
  const [prompt, setPrompt] = useState("");

  let [
    getOwnedNfts,
    { data: ownedNftsData, called: ownedNftsCalled, loading: ownedNftsLoading },
  ] = useLazyQuery(GET_OWNED_NFTS, {
    fetchPolicy: "network-only",
    variables: { owner: `User:${owner}` },
  });

  const [transferNft, { loading: transferLoading }] = useMutation(TRANSFER_NFT, {
    onError: (error) => setTransferError("Transfer Error: " + error.message),
    onCompleted: () => {
      handleTransferClose();
      getOwnedNfts(); // Refresh owned NFTs list
    },
  });

  const [mintNft, { loading: mintLoading }] = useMutation(MINT_NFT, {
    onError: (error) => setMintError("Mint Error: " + error.message),
    onCompleted: () => {
      handleMintClose();
      getOwnedNfts(); // Refresh owned NFTs list
    },
  });

  const [previewStory, { loading: previewLoading }] = useLazyQuery(PROMPT, {
    onError: (error) => setMintError("Preview error: " + error.message),
    onCompleted: (data) => {
      setPromptPreview(data.prompt);
    },
  });

  if (!ownedNftsCalled) {
    void getOwnedNfts();
  }

  useSubscription(NOTIFICATION_SUBSCRIPTION, {
    variables: { chainId: chainId },
    onData: () => getOwnedNfts(), // Refresh on new notifications
  });

  const handleMintOpen = () => setMintOpen(true);
  const handleMintClose = () => {
    setMintOpen(false);
    resetMintDialog();
  }

  const handleTransferOpen = (token_id) => {
    setTokenID(token_id);
    setTransferOpen(true);
  };
  const handleTransferClose = () => {
    setTransferOpen(false);
    resetTransferDialog();
  }

  const resetMintDialog = () => {
    setMintError("");
    mintForm.resetFields();
  };

  // Placeholder for form submission logic
  const handleMintSubmit = () => {

    mintNft({
      variables: {
        minter: `User:${owner}`,
        prompt: prompt,
      },
    }).then(r => console.log("NFT minted: " + JSON.stringify(r, null, 2)));
  };

  const handlePreview = () => {
    setPromptPreview(undefined);
    previewStory(
        {
          variables: {
            prompt: prompt
          }
        }
    ).then(r => {
      console.log(r)
      setPromptPreview(r.data.prompt)
    });
  };

  const resetTransferDialog = () => {
    setTokenID("");
    setTargetChainID("");
    setTargetOwner("");
    setTransferError("");
    transferForm.resetFields();
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
  };

  const onTransferValuesChange = (values) => {
    if (values.target_chain_id !== undefined) {
      setTargetChainID(values.target_chain_id);
    }

    if (values.target_owner !== undefined) {
      setTargetOwner(values.target_owner);
    }
  };

  const onMintValuesChange = (values) => {
    if (values.story !== undefined) {
      setPrompt(values.story);
    }
  };

  const columns = [
    {
      title: 'Token Id',
      dataIndex: 'token_id',
      key: 'token_id',
    },
    {
      title: 'Minter',
      dataIndex: 'minter',
      key: 'minter',
    },
    {
      title: 'Prompt',
      dataIndex: 'prompt',
      key: 'prompt',
      render: (_, nft) => (
        <>
        {nft.prompt}
        </>
      ),
    },
    {
      title: 'Transfer',
      dataIndex: 'transfer',
      key: 'transfer',
      render: (_, nft) => (
        <Button onClick={() => handleTransferOpen(nft.token_id)}>Transfer</Button>
      ),
    }
  ];

  const userInfoItems = [
    {
      key: 'account',
      label: 'Account',
      children: owner,
    },
    {
      key: 'chain',
      label: 'Chain',
      children: chainId,
    }
  ];

  return (
    <Layout sx={{ mt: 4, overflowX: 'auto' }}>
      <Card sx={{ minWidth: 'auto', width: '100%', mx: 'auto', my: 2 }}>
        <Title>Linera NFT</Title>
        <Space
          direction="vertical"
          style={{
            display: 'flex',
          }}
        >
          <Descriptions title="User Info" items={userInfoItems} column={1} />

          <Typography style={{ fontWeight: 'bold' }} variant="h6">Your Owned NFTs:</Typography>
          <Table columns={columns} loading={ownedNftsLoading} dataSource={ownedNftsData ? Object.entries(ownedNftsData.ownedNfts).map(([token_id, nft]) => {
            return {
              key: token_id,
              token_id: token_id,
              name: nft.name,
              minter: nft.minter,
              prompt: nft.prompt,
            };
          }) : []} />

          <Button type="primary" onClick={handleMintOpen}>Mint</Button>
        </Space>

        <Modal title="Mint NFT" open={isMintOpen} footer={null} onCancel={handleMintClose}>
          <Form
            name="mint"
            labelCol={{
              span: 8,
            }}
            wrapperCol={{
              span: 16,
            }}
            style={{
              maxWidth: 600,
            }}
            form={mintForm}
            autoComplete="off"
            onValuesChange={onMintValuesChange}
            disabled={mintLoading}
          >
            <Space
                direction="vertical"
                style={{
                  display: 'flex',
                }}
            >
              {mintError ? (
                  <Alert
                      message="Error"
                      description={mintError}
                      type="error"
                      showIcon
                  />
              ) : null}
              <Form.Item
                  label="Story"
                  name="story"
                  rules={[
                    {
                      required: true,
                      message: 'Please start your story!',
                    },
                  ]}
              >
                <Input/>
              </Form.Item>
              {
                promptPreview ? (
                    <Form.Item
                        label="Preview"
                        rules={[
                          {
                            required: true,
                            message: 'Please start your story!',
                          },
                        ]}
                    >
                      {promptPreview}
                    </Form.Item>
                ) : null
              }
              {
                previewLoading ? (
                    <Loader/>
              ) : null
              }
            </Space>
            <Form.Item
                wrapperCol={{
                  offset: 8,
                  span: 16,
                }}
            >
              <Space>
                <Button type="primary" onClick={handleMintSubmit} loading={mintLoading}>
                  Submit
                </Button>
                <Button type="primary" onClick={handlePreview} loading={mintLoading}>
                  Preview
                </Button>
                <Button onClick={handleMintClose}>
                Cancel
                </Button>
              </Space>
            </Form.Item>
          </Form>
        </Modal>

        <Modal title="Transfer NFT" open={isTransferOpen} footer={null} onCancel={handleTransferClose}>
          <Form name="transfer"
            labelCol={{
              span: 8,
            }}
            wrapperCol={{
              span: 16,
            }}
            style={{
              maxWidth: 600,
            }}
            initialValues={{
              token_id: tokenID,
            }}
            form={transferForm}
            autoComplete="off"
            onValuesChange={onTransferValuesChange}
            disabled={transferLoading}
          >
            {transferError ? (
              <Alert
                message="Error"
                description={transferError}
                type="error"
                showIcon
              />
            ) : null}
            <Form.Item
              label="Token Id"
              name="token_id"
              rules={[
                {
                  required: true,
                  message: 'Please input the token id!',
                },
              ]}
            >
              <Input disabled />
            </Form.Item>

            <Form.Item
              label="Target Chain Id"
              name="target_chain_id"
              rules={[
                {
                  required: true,
                  message: 'Please input the target chain ID!',
                },
              ]}
            >
              <Input />
            </Form.Item>

            <Form.Item
              label="Target Owner"
              name="target_owner"
              rules={[
                {
                  required: true,
                  message: 'Please input the target owner!',
                },
              ]}
            >
              <Input />
            </Form.Item>

            <Form.Item
              wrapperCol={{
                offset: 8,
                span: 16,
              }}
            >
              <Space>
                <Button type="primary" onClick={handleTransferSubmit} loading={transferLoading}>
                  Submit
                </Button>
                <Button onClick={handleTransferClose}>
                  Cancel
                </Button>
              </Space>
            </Form.Item>
          </Form>
        </Modal>
      </Card>
    </Layout >
  );
}

export default App;
