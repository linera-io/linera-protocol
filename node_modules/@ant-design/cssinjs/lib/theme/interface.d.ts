export type TokenType = object;
export type DerivativeFunc<DesignToken extends TokenType, DerivativeToken extends TokenType> = (designToken: DesignToken, derivativeToken?: DerivativeToken) => DerivativeToken;
