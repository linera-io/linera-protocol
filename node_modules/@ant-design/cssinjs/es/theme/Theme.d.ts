import type { DerivativeFunc, TokenType } from './interface';
/**
 * Theme with algorithms to derive tokens from design tokens.
 * Use `createTheme` first which will help to manage the theme instance cache.
 */
export default class Theme<DesignToken extends TokenType, DerivativeToken extends TokenType> {
    private derivatives;
    readonly id: number;
    constructor(derivatives: DerivativeFunc<DesignToken, DerivativeToken> | DerivativeFunc<DesignToken, DerivativeToken>[]);
    getDerivativeToken(token: DesignToken): DerivativeToken;
}
