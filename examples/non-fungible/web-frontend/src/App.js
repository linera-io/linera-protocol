import { useState } from 'react';
import React from 'react';
import {
  gql,
  useMutation,
  useLazyQuery,
  useSubscription,
} from '@apollo/client';
import {
  Card,
  Typography,
  Button,
  Table,
  Layout,
  Modal,
  Form,
  Input,
  Space,
  Alert,
  Descriptions,
  Upload,
  Image,
} from 'antd';
import { PlusOutlined } from '@ant-design/icons';
import { useClient } from './GraphQLProvider';

const { Title } = Typography;

const GET_OWNED_NFTS = gql`
  query OwnedNfts($owner: AccountOwner!) {
    ownedNfts(owner: $owner)
  }
`;

const MINT_NFT = gql`
  mutation Mint($minter: AccountOwner!, $name: String!, $blobId: BlobId!) {
    mint(minter: $minter, name: $name, blobId: $blobId)
  }
`;

const PUBLISH_DATA_BLOB = gql`
  mutation PublishDataBlob($chainId: ChainId!, $blobContent: BlobContent!) {
    publishDataBlob(chainId: $chainId, blobContent: $blobContent)
  }
`;

const TRANSFER_NFT = gql`
  mutation Transfer(
    $sourceOwner: AccountOwner!
    $tokenId: String!
    $targetAccount: FungibleAccount!
  ) {
    transfer(
      sourceOwner: $sourceOwner
      tokenId: $tokenId
      targetAccount: $targetAccount
    )
  }
`;

const NOTIFICATION_SUBSCRIPTION = gql`
  subscription Notifications($chainId: ID!) {
    notifications(chainId: $chainId)
  }
`;

const getFileBase64 = (file) =>
  new Promise((resolve, reject) => {
    const reader = new FileReader();
    reader.readAsDataURL(file);
    reader.onload = () => resolve(reader.result);
    reader.onerror = (error) => reject(error);
  });

const normFile = (e) => {
  return e?.fileList[0];
};

function App({ chainId, owner }) {
  const { appClient, nodeServiceClient } = useClient();
  // Error
  const [transferError, setTransferError] = useState('');
  const [mintError, setMintError] = useState('');

  // Dialog control
  const [isMintOpen, setMintOpen] = useState(false);
  const [isTransferOpen, setTransferOpen] = useState(false);

  // Transfer dialog
  const [tokenID, setTokenID] = useState('');
  const [targetChainID, setTargetChainID] = useState('');
  const [targetOwner, setTargetOwner] = useState('');
  const [transferForm] = Form.useForm();

  // Mint dialog
  const [name, setName] = useState('');
  const [mintForm] = Form.useForm();
  const [mintPreviewOpen, setMintPreviewOpen] = useState(false);
  const [mintPreviewImage, setMintPreviewImage] = useState('');
  const [mintPreviewTitle, setMintPreviewTitle] = useState('');
  const [mintUploadedFileList, setMintUploadedFileList] = useState([]);
  const [mintImageUrl, setMintImageUrl] = useState('');

  let [
    getOwnedNfts,
    { data: ownedNftsData, called: ownedNftsCalled, loading: ownedNftsLoading },
  ] = useLazyQuery(GET_OWNED_NFTS, {
    client: appClient,
    fetchPolicy: 'network-only',
    variables: { owner: `User:${owner}` },
  });

  const [transferNft, { loading: transferLoading }] = useMutation(
    TRANSFER_NFT,
    {
      client: appClient,
      onError: (error) => setTransferError('Transfer Error: ' + error.message),
      onCompleted: () => {
        handleTransferClose();
        getOwnedNfts(); // Refresh owned NFTs list
      },
    }
  );

  const [publishDataBlob, { loading: publishDataBlobLoading }] = useMutation(
    PUBLISH_DATA_BLOB,
    {
      client: nodeServiceClient,
      onError: (error) =>
        setMintError('Publish Data Blob Error: ' + error.message),
      onCompleted: () => {},
    }
  );

  const [mintNft, { loading: mintLoading }] = useMutation(MINT_NFT, {
    client: appClient,
    onError: (error) => setMintError('Mint Error: ' + error.message),
    onCompleted: () => {
      handleMintClose();
      getOwnedNfts(); // Refresh owned NFTs list
    },
  });

  if (!ownedNftsCalled) {
    void getOwnedNfts();
  }

  useSubscription(NOTIFICATION_SUBSCRIPTION, {
    client: appClient,
    variables: { chainId: chainId },
    onData: () => getOwnedNfts(), // Refresh on new notifications
  });

  const handleMintOpen = () => setMintOpen(true);
  const handleMintClose = () => {
    setMintOpen(false);
    resetMintDialog();
  };

  const handleTransferOpen = (token_id) => {
    setTokenID(token_id);
    setTransferOpen(true);
  };
  const handleTransferClose = () => {
    setTransferOpen(false);
    resetTransferDialog();
  };

  const resetMintDialog = () => {
    setName('');
    setMintError('');
    setMintUploadedFileList([]);
    setMintImageUrl('');
    mintForm.resetFields();
  };

  // Placeholder for form submission logic
  const handleMintSubmit = () => {
    const encoder = new TextEncoder();
    const byteArrayFile = encoder.encode(mintImageUrl);

    publishDataBlob({
      variables: {
        chainId: chainId,
        blobContent: {
          bytes: Array.from(byteArrayFile)
        },
      },
    }).then((r) => {
      if ('errors' in r) {
        console.log('Got error while publishing Data Blob: ' + JSON.stringify(r, null, 2));
      } else {
        console.log('Data Blob published: ' + JSON.stringify(r, null, 2));
        const blobId = r['data']['publishDataBlob'];
        mintNft({
          variables: {
            minter: `User:${owner}`,
            name: name,
            blobId: blobId,
          },
        }).then((r) => {
          if ('errors' in r) {
            console.log('Got error while minting NFT: ' + JSON.stringify(r, null, 2));
          } else {
            console.log('NFT minted: ' + JSON.stringify(r, null, 2));
          }
        });
      }
    });
  };

  const resetTransferDialog = () => {
    setTokenID('');
    setTargetChainID('');
    setTargetOwner('');
    setTransferError('');
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
        },
      },
    }).then((r) => {
      if ('errors' in r) {
        console.log('Error while transferring NFT: ' + JSON.stringify(r, null, 2));
      } else {
        console.log('NFT transferred: ' + JSON.stringify(r, null, 2));
      }
    });
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
    if (values.name !== undefined) {
      setName(values.name);
    }
  };

  const handleMintPreviewCancel = () => setMintPreviewOpen(false);
  const handleMintPreview = (file) => {
    setMintPreviewImage(file.url || file.preview);
    setMintPreviewOpen(true);
    setMintPreviewTitle(
      file.name || file.url.substring(file.url.lastIndexOf('/') + 1)
    );
  };

  const handleUploadChange = async ({ fileList: newFileList }) => {
    if (newFileList.length > 0) {
      const imageDataUrl = await getFileBase64(newFileList[0].originFileObj);
      newFileList[0].preview = imageDataUrl;
      if (newFileList[0] !== undefined) {
        delete newFileList[0].error;
        newFileList[0].status = 'done';
      }
      setMintImageUrl(imageDataUrl);
    } else {
      setMintImageUrl('');
    }
    setMintUploadedFileList(newFileList);
  };

  const uploadButton = (
    <button
      style={{
        border: 0,
        background: 'none',
      }}
      type='button'
    >
      <PlusOutlined />
      <div
        style={{
          marginTop: 8,
        }}
      >
        Upload
      </div>
    </button>
  );

  const columns = [
    {
      title: 'Token Id',
      dataIndex: 'token_id',
      key: 'token_id',
    },
    {
      title: 'Name',
      dataIndex: 'name',
      key: 'name',
    },
    {
      title: 'Minter',
      dataIndex: 'minter',
      key: 'minter',
    },
    {
      title: 'Image',
      dataIndex: 'image',
      key: 'image',
      render: (_, nft) => (
        <Image
          width={80}
          height={80}
          src={nft.payload}
          fallback='data:image/png;base64,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'
        />
      ),
    },
    {
      title: 'Transfer',
      dataIndex: 'transfer',
      key: 'transfer',
      render: (_, nft) => (
        <Button onClick={() => handleTransferOpen(nft.token_id)}>
          Transfer
        </Button>
      ),
    },
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
    },
  ];

  return (
    <Layout sx={{ mt: 4, overflowX: 'auto' }}>
      <Card sx={{ minWidth: 'auto', width: '100%', mx: 'auto', my: 2 }}>
        <Title>Linera NFT</Title>
        <Space
          direction='vertical'
          style={{
            display: 'flex',
          }}
        >
          <Descriptions title='User Info' items={userInfoItems} column={1} />

          <Typography style={{ fontWeight: 'bold' }} variant='h6'>
            Your Owned NFTs:
          </Typography>
          <Table
            columns={columns}
            loading={ownedNftsLoading}
            dataSource={
              ownedNftsData
                ? Object.entries(ownedNftsData.ownedNfts).map(
                    ([token_id, nft]) => {
                      const decoder = new TextDecoder();
                      const deserializedImage = decoder.decode(
                        new Uint8Array(nft.payload)
                      );

                      return {
                        key: token_id,
                        token_id: token_id,
                        name: nft.name,
                        minter: nft.minter,
                        payload: deserializedImage,
                      };
                    }
                  )
                : []
            }
          />

          <Button type='primary' onClick={handleMintOpen}>
            Mint
          </Button>
        </Space>

        <Modal
          title='Mint NFT'
          open={isMintOpen}
          footer={null}
          onCancel={handleMintClose}
        >
          <Form
            name='mint'
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
            autoComplete='off'
            onValuesChange={onMintValuesChange}
            disabled={mintLoading || publishDataBlobLoading}
          >
            <Space
              direction='vertical'
              style={{
                display: 'flex',
              }}
            >
              {mintError ? (
                <Alert
                  message='Error'
                  description={mintError}
                  type='error'
                  showIcon
                />
              ) : null}
              <Form.Item
                label='Name'
                name='name'
                rules={[
                  {
                    required: true,
                    message: 'Please input the name!',
                  },
                ]}
              >
                <Input />
              </Form.Item>
            </Space>

            <Form.Item label='Image'>
              <Form.Item
                rules={[
                  {
                    required: true,
                    message: 'Please input the image!',
                  },
                ]}
                name='image'
                valuePropName='imageList'
                getValueFromEvent={normFile}
                noStyle
              >
                <Upload
                  listType='picture-card'
                  fileList={mintUploadedFileList}
                  onPreview={handleMintPreview}
                  onChange={handleUploadChange}
                  accept='image/*'
                >
                  {mintUploadedFileList.length >= 1 ? null : uploadButton}
                </Upload>
              </Form.Item>
            </Form.Item>

            <Form.Item
              wrapperCol={{
                offset: 8,
                span: 16,
              }}
            >
              <Space>
                <Button
                  type='primary'
                  onClick={handleMintSubmit}
                  loading={mintLoading || publishDataBlobLoading}
                >
                  Submit
                </Button>
                <Button onClick={handleMintClose}>Cancel</Button>
              </Space>
            </Form.Item>
          </Form>
        </Modal>

        <Modal
          open={mintPreviewOpen}
          title={mintPreviewTitle}
          footer={null}
          onCancel={handleMintPreviewCancel}
        >
          <img
            alt=''
            style={{
              width: '100%',
            }}
            src={mintPreviewImage}
          />
        </Modal>

        <Modal
          title='Transfer NFT'
          open={isTransferOpen}
          footer={null}
          onCancel={handleTransferClose}
        >
          <Form
            name='transfer'
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
            autoComplete='off'
            onValuesChange={onTransferValuesChange}
            disabled={transferLoading}
          >
            {transferError ? (
              <Alert
                message='Error'
                description={transferError}
                type='error'
                showIcon
              />
            ) : null}
            <Form.Item
              label='Token Id'
              name='token_id'
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
              label='Target Chain Id'
              name='target_chain_id'
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
              label='Target Owner'
              name='target_owner'
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
                <Button
                  type='primary'
                  onClick={handleTransferSubmit}
                  loading={transferLoading}
                >
                  Submit
                </Button>
                <Button onClick={handleTransferClose}>Cancel</Button>
              </Space>
            </Form.Item>
          </Form>
        </Modal>
      </Card>
    </Layout>
  );
}

export default App;
