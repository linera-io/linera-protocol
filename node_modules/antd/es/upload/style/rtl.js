// =========================== Motion ===========================
const genRtlStyle = token => {
  const {
    componentCls
  } = token;
  return {
    [`${componentCls}-rtl`]: {
      direction: 'rtl'
    }
  };
};
export default genRtlStyle;