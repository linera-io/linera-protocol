/**
 * Fallback of IE.
 * Safe to remove.
 */
// Style as inline component
import { prepareToken } from '.';
import { genSubStyleComponent } from '../../theme/internal';
// ============================= Fallback =============================
const genFallbackStyle = token => {
  const {
    formItemCls
  } = token;
  return {
    '@media screen and (-ms-high-contrast: active), (-ms-high-contrast: none)': {
      // Fallback for IE, safe to remove we not support it anymore
      [`${formItemCls}-control`]: {
        display: 'flex'
      }
    }
  };
};
// ============================== Export ==============================
export default genSubStyleComponent(['Form', 'item-item'], (token, _ref) => {
  let {
    rootPrefixCls
  } = _ref;
  const formToken = prepareToken(token, rootPrefixCls);
  return [genFallbackStyle(formToken)];
});