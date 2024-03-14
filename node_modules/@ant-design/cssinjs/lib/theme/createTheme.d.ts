import Theme from './Theme';
import type { DerivativeFunc, TokenType } from './interface';
/**
 * Same as new Theme, but will always return same one if `derivative` not changed.
 */
export default function createTheme<DesignToken extends TokenType, DerivativeToken extends TokenType>(derivatives: DerivativeFunc<DesignToken, DerivativeToken>[] | DerivativeFunc<DesignToken, DerivativeToken>): Theme<any, any>;
