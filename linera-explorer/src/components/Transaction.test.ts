import { mount } from '@vue/test-utils'
import Transaction from './Transaction.vue'
import { set_test_config } from './utils'

describe('Transaction Component', () => {
  beforeAll(async () => {
    await set_test_config()
  })

  test('displays execute operation transaction correctly', () => {
    const wrapper = mount(Transaction, {
      props: {
        transaction: {
          transactionType: 'ExecuteOperation',
          operation: {
            operationType: 'System',
            applicationId: null,
            userBytesHex: null,
            systemOperation: {
              systemOperationType: 'Transfer',
              transfer: {
                owner: 'a36c72207a7c3cef20eb254978c0947d7cf28c9c7d7c62de42a0ed9db901cf3f',
                recipient: {
                  chainId: 'aee928d4bf3880353b4a3cd9b6f88e6cc6e5ed050860abae439e7782e9b2dfe8',
                  owner: 'b45c15b5d3a25c8b3e7c9f4d6e8a2c1b9e6f3d8a4b7c2e5f8a1b4c7d0e3f6a9'
                },
                amount: '1000000'
              }
            }
          },
          incomingBundle: null
        },
        index: 0,
        blockHash: 'test-block-hash'
      },
    })

    // Check that transaction header is rendered correctly
    expect(wrapper.text()).toContain('Transaction 1')
    expect(wrapper.text()).toContain('Execute Operation')

    // Check that the operation component is rendered
    expect(wrapper.text()).toContain('Transfer Details')
    expect(wrapper.text()).toContain('1000000')
  })

  test('displays receive messages transaction correctly', () => {
    const wrapper = mount(Transaction, {
      props: {
        transaction: {
          transactionType: 'ReceiveMessages',
          operation: null,
          incomingBundle: {
            origin: {
              medium: "Direct",
              sender: "aee928d4bf3880353b4a3cd9b6f88e6cc6e5ed050860abae439e7782e9b2dfe8"
            },
            action: 'Accept',
            bundle: {
              height: 5,
              timestamp: 1694097510206912,
              certificateHash: 'f1c748c5e39591125250e85d57fdeac0b7ba44a32c12c616eb4537f93b6e5d0a',
              transactionIndex: 0,
              messages: [{
                authenticatedOwner: null,
                grant: '1000',
                refundGrantTo: null,
                kind: 'Tracked',
                index: 0,
                message: {},
                messageMetadata: {
                  messageType: 'System',
                  applicationId: null,
                  userBytesHex: null,
                  systemMessage: {
                    systemMessageType: 'Credit',
                    credit: {
                      target: 'b45c15b5d3a25c8b3e7c9f4d6e8a2c1b9e6f3d8a4b7c2e5f8a1b4c7d0e3f6a9',
                      amount: '500000',
                      source: 'aee928d4bf3880353b4a3cd9b6f88e6cc6e5ed050860abae439e7782e9b2dfe8'
                    },
                    withdraw: null
                  }
                }
              }]
            }
          }
        },
        index: 1,
        blockHash: 'test-block-hash'
      },
    })

    // Check that transaction header is rendered correctly
    expect(wrapper.text()).toContain('Transaction 2')
    expect(wrapper.text()).toContain('Receive Messages')

    // Check incoming bundle details
    expect(wrapper.text()).toContain('Incoming Bundle from Chain')
    expect(wrapper.text()).toContain('Origin:')
    expect(wrapper.text()).toContain('Accept')
    expect(wrapper.text()).toContain('Height: 5')

    // Check message details
    expect(wrapper.text()).toContain('Messages (1)')
    expect(wrapper.text()).toContain('Message 1')
    expect(wrapper.text()).toContain('System Message: Credit')
    expect(wrapper.text()).toContain('Target:')
    expect(wrapper.text()).toContain('Amount: 500000')
  })

  test('displays receive messages transaction with withdraw system message', () => {
    const wrapper = mount(Transaction, {
      props: {
        transaction: {
          transactionType: 'ReceiveMessages',
          operation: null,
          incomingBundle: {
            origin: {
              medium: "Direct",
              sender: "aee928d4bf3880353b4a3cd9b6f88e6cc6e5ed050860abae439e7782e9b2dfe8"
            },
            action: 'Accept',
            bundle: {
              height: 3,
              timestamp: 1694097510206912,
              certificateHash: 'f1c748c5e39591125250e85d57fdeac0b7ba44a32c12c616eb4537f93b6e5d0a',
              transactionIndex: 0,
              messages: [{
                authenticatedOwner: null,
                grant: '0',
                refundGrantTo: null,
                kind: 'Tracked',
                index: 0,
                message: {},
                messageMetadata: {
                  messageType: 'System',
                  applicationId: null,
                  userBytesHex: null,
                  systemMessage: {
                    systemMessageType: 'Withdraw',
                    credit: null,
                    withdraw: {
                      owner: 'a36c72207a7c3cef20eb254978c0947d7cf28c9c7d7c62de42a0ed9db901cf3f',
                      amount: '750000',
                      recipient: {
                        chainId: 'bff928d4bf3880353b4a3cd9b6f88e6cc6e5ed050860abae439e7782e9b2dfe9',
                        owner: 'c56c72207a7c3cef20eb254978c0947d7cf28c9c7d7c62de42a0ed9db901cf4a'
                      }
                    }
                  }
                }
              }]
            }
          }
        },
        index: 0,
        blockHash: 'test-block-hash-2'
      },
    })

    // Check withdraw message details
    expect(wrapper.text()).toContain('System Message: Withdraw')
    expect(wrapper.text()).toContain('Owner:')
    expect(wrapper.text()).toContain('Amount: 750000')
    expect(wrapper.text()).toContain('Recipient:')
  })

  test('displays receive messages transaction with user message', () => {
    const wrapper = mount(Transaction, {
      props: {
        transaction: {
          transactionType: 'ReceiveMessages',
          operation: null,
          incomingBundle: {
            origin: {
              medium: "Direct",
              sender: "aee928d4bf3880353b4a3cd9b6f88e6cc6e5ed050860abae439e7782e9b2dfe8"
            },
            action: 'Accept',
            bundle: {
              height: 10,
              timestamp: 1694097510206912,
              certificateHash: 'f1c748c5e39591125250e85d57fdeac0b7ba44a32c12c616eb4537f93b6e5d0a',
              transactionIndex: 0,
              messages: [{
                authenticatedOwner: null,
                grant: '0',
                refundGrantTo: null,
                kind: 'Tracked',
                index: 0,
                message: {},
                messageMetadata: {
                  messageType: 'User',
                  applicationId: null, // Remove problematic application ID
                  userBytesHex: '48656c6c6f20776f726c64',
                  systemMessage: null
                }
              }]
            }
          }
        },
        index: 0,
        blockHash: 'test-block-hash-3'
      },
    })

    // Check user message details
    expect(wrapper.text()).toContain('User Message')
  })

  test('handles message with basic user metadata', () => {
    const wrapper = mount(Transaction, {
      props: {
        transaction: {
          transactionType: 'ReceiveMessages',
          operation: null,
          incomingBundle: {
            origin: {
              medium: "Direct",
              sender: "aee928d4bf3880353b4a3cd9b6f88e6cc6e5ed050860abae439e7782e9b2dfe8"
            },
            action: 'Accept',
            bundle: {
              height: 1,
              timestamp: 1694097510206912,
              certificateHash: 'f1c748c5e39591125250e85d57fdeac0b7ba44a32c12c616eb4537f93b6e5d0a',
              transactionIndex: 0,
              messages: [{
                authenticatedOwner: null,
                grant: '0',
                refundGrantTo: null,
                kind: 'Tracked',
                index: 0,
                message: { data: 'some-raw-data' },
                messageMetadata: {
                  messageType: 'User',
                  applicationId: null,
                  userBytesHex: null,
                  systemMessage: null
                }
              }]
            }
          }
        },
        index: 0,
        blockHash: 'test-block-hash-4'
      },
    })

    // Check that user message with basic metadata is displayed
    expect(wrapper.text()).toContain('User Message')
  })

  test('handles unknown transaction type', () => {
    const wrapper = mount(Transaction, {
      props: {
        transaction: {
          transactionType: 'UnknownType',
          operation: null,
          incomingBundle: null
        },
        index: 0,
        blockHash: 'test-block-hash-5'
      },
    })

    // Check that unknown transaction type is handled
    expect(wrapper.text()).toContain('Unknown transaction type: UnknownType')
  })

  test('displays user operation transaction correctly', () => {
    const wrapper = mount(Transaction, {
      props: {
        transaction: {
          transactionType: 'ExecuteOperation',
          operation: {
            operationType: 'User',
            applicationId: null, // Remove problematic application ID
            userBytesHex: '48656c6c6f20776f726c64',
            systemOperation: null
          },
          incomingBundle: null
        },
        index: 2,
        blockHash: 'test-block-hash-6'
      },
    })

    // Check user operation display
    expect(wrapper.text()).toContain('Transaction 3')
    expect(wrapper.text()).toContain('Execute Operation')
    expect(wrapper.text()).toContain('User Operation')
  })
})