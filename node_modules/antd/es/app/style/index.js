import { genStyleHooks } from '../../theme/internal';
// =============================== Base ===============================
const genBaseStyle = token => {
  const {
    componentCls,
    colorText,
    fontSize,
    lineHeight,
    fontFamily
  } = token;
  return {
    [componentCls]: {
      color: colorText,
      fontSize,
      lineHeight,
      fontFamily
    }
  };
};
export const prepareComponentToken = () => ({});
// ============================== Export ==============================
export default genStyleHooks('App', genBaseStyle, prepareComponentToken);