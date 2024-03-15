import type { CSSObject } from '@ant-design/cssinjs';
import type { AliasToken, PresetColorKey } from '../internal';
import type { TokenWithCommonCls } from './genComponentStyleHook';
interface CalcColor {
    /** token[`${colorKey}-1`] */
    lightColor: string;
    /** token[`${colorKey}-3`] */
    lightBorderColor: string;
    /** token[`${colorKey}-6`] */
    darkColor: string;
    /** token[`${colorKey}-7`] */
    textColor: string;
}
type GenCSS = (colorKey: PresetColorKey, calcColor: CalcColor) => CSSObject;
export default function genPresetColor<Token extends TokenWithCommonCls<AliasToken>>(token: Token, genCss: GenCSS): CSSObject;
export {};
