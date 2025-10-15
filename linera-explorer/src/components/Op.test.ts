import { mount } from '@vue/test-utils'
import Op from './Op.vue'
import { set_test_config } from './utils'

describe('Op Component', () => {
  beforeAll(async () => {
    await set_test_config()
  })

  test('mounts with legacy system operation', () => {
    mount(Op, {
      props: {
        id: 'op',
        op: {
          operationType: 'System',
          systemOperation: {
            systemOperationType: 'PublishModule',
            publishModule: {
              moduleId: "e476187f6ddfeb9d588c7b45d3df334d5501d6499b3f9ad5595cae86cce16a65020000000000000000000000"
            }
          }
        }
      },
    })
  })

  test('displays transfer system operation correctly', () => {
    const wrapper = mount(Op, {
      props: {
        id: 'transfer-op',
        op: {
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
        }
      },
    })

    // Check that transfer details are rendered
    expect(wrapper.text()).toContain('Transfer Details')
    expect(wrapper.text()).toContain('From:')
    expect(wrapper.text()).toContain('To:')
    expect(wrapper.text()).toContain('Amount:')
    expect(wrapper.text()).toContain('1000000')
    expect(wrapper.text()).toContain('a36c72207a7c3cef20eb254978c0947d7cf28c9c7d7c62de42a0ed9db901cf3f')
  })

  test('displays claim system operation correctly', () => {
    const wrapper = mount(Op, {
      props: {
        id: 'claim-op',
        op: {
          operationType: 'System',
          applicationId: null,
          userBytesHex: null,
          systemOperation: {
            systemOperationType: 'Claim',
            claim: {
              owner: 'a36c72207a7c3cef20eb254978c0947d7cf28c9c7d7c62de42a0ed9db901cf3f',
              targetId: 'aee928d4bf3880353b4a3cd9b6f88e6cc6e5ed050860abae439e7782e9b2dfe8',
              recipient: {
                chainId: 'bff928d4bf3880353b4a3cd9b6f88e6cc6e5ed050860abae439e7782e9b2dfe9',
                owner: 'c56c72207a7c3cef20eb254978c0947d7cf28c9c7d7c62de42a0ed9db901cf4a'
              },
              amount: '500000'
            }
          }
        }
      },
    })

    // Check that claim details are rendered
    expect(wrapper.text()).toContain('Claim Details')
    expect(wrapper.text()).toContain('Owner:')
    expect(wrapper.text()).toContain('Target Chain:')
    expect(wrapper.text()).toContain('Recipient:')
    expect(wrapper.text()).toContain('Amount:')
    expect(wrapper.text()).toContain('500000')
  })

  test('displays open chain system operation correctly', () => {
    const wrapper = mount(Op, {
      props: {
        id: 'open-chain-op',
        op: {
          operationType: 'System',
          applicationId: null,
          userBytesHex: null,
          systemOperation: {
            systemOperationType: 'OpenChain',
            openChain: {
              balance: '2000000'
            }
          }
        }
      },
    })

    // Check that open chain details are rendered
    expect(wrapper.text()).toContain('Open Chain Details')
    expect(wrapper.text()).toContain('Initial Balance:')
    expect(wrapper.text()).toContain('2000000')
  })

  test('displays user operation correctly', () => {
    const wrapper = mount(Op, {
      props: {
        id: 'user-op',
        op: {
          operationType: 'User',
          applicationId: null, // Remove problematic application ID
          userBytesHex: '48656c6c6f20776f726c64'
        }
      },
    })

    // Check that user operation details are rendered
    expect(wrapper.text()).toContain('User Operation')
    expect(wrapper.text()).toContain('Operation Data (hex):')
  })

  test('shows warning when no structured data is available', () => {
    const wrapper = mount(Op, {
      props: {
        id: 'unknown-op',
        op: {
          operationType: 'System',
          applicationId: null,
          userBytesHex: null
          // Missing systemOperation field
        }
      },
    })

    // Check that warning is displayed
    expect(wrapper.text()).toContain('Warning: No structured operation data available')
  })

  test('handles unknown operation type', () => {
    const wrapper = mount(Op, {
      props: {
        id: 'unknown-type-op',
        op: {
          operationType: 'Unknown',
          someField: 'someValue'
        }
      },
    })

    // Check that unknown operation is handled
    expect(wrapper.text()).toContain('Unknown Operation')
    expect(wrapper.text()).toContain('Operation Type: Unknown')
  })
})