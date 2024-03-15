import { genStyleHooks } from '../../theme/internal';
// ============================== Shared ==============================
const genSharedAffixStyle = token => {
  const {
    componentCls
  } = token;
  return {
    [componentCls]: {
      position: 'fixed',
      zIndex: token.zIndexPopup
    }
  };
};
export const prepareComponentToken = token => ({
  zIndexPopup: token.zIndexBase + 10
});
// ============================== Export ==============================
export default genStyleHooks('Affix', genSharedAffixStyle, prepareComponentToken);