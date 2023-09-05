import { ApplicationOverview } from '../gql/service'

export interface Application {
  app: ApplicationOverview,
  queries: any[],
  mutations: any[],
  subscriptions: any[],
}

export interface Plugin {
  name: string,
  link: string,
  queries: any[],
}

export interface IntrospectionFull {
  kind: string,
  name: string,
  description?: string,
  fields?: IntrospectionField[],
  inputFields?: IntrospectionInputValue[],
  interfaces?: IntrospectionFull[],
  enumValues?: IntrospectionEnum[],
  ofType?: IntrospectionFull,
  // added fields to compute request
  _input?: any,
  _include?: boolean,
}

export interface IntrospectionField {
  name: string,
  description?: string,
  args: IntrospectionInputValue[],
  type: IntrospectionFull,
}

export interface IntrospectionInputValue {
  name: string,
  description?: string,
  type: IntrospectionFull
  defaultValue?: any,
}

export interface IntrospectionEnum {
  name: string,
  description: string,
}
