import type { FullToken, GenerateStyle } from '../../theme/internal';
/** Component only token. Which will handle additional calculation of alias token */
export interface ComponentToken {
}
interface SpaceToken extends FullToken<'Space'> {
}
declare const genSpaceCompactStyle: GenerateStyle<SpaceToken>;
export default genSpaceCompactStyle;
