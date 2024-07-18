import { Social } from './__generated__/graphql'

export interface ReceivedPosts {
  value: Social['receivedPosts']['entries'][0]['value']
}
