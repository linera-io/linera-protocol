import { unit } from '@ant-design/cssinjs';
import { genBorderlessStyle, genFilledStyle, genOutlinedStyle } from '../../input/style/variants';
const genVariantsStyle = token => {
  const {
    componentCls
  } = token;
  return {
    [componentCls]: [Object.assign(Object.assign(Object.assign({}, genOutlinedStyle(token)), genFilledStyle(token)), genBorderlessStyle(token)),
    // ========================= Multiple =========================
    {
      '&-outlined': {
        [`&${componentCls}-multiple ${componentCls}-selection-item`]: {
          background: token.multipleItemBg,
          border: `${unit(token.lineWidth)} ${token.lineType} ${token.multipleItemBorderColor}`
        }
      },
      '&-filled': {
        [`&${componentCls}-multiple ${componentCls}-selection-item`]: {
          background: token.colorBgContainer,
          border: `${unit(token.lineWidth)} ${token.lineType} ${token.colorSplit}`
        }
      },
      '&-borderless': {
        [`&${componentCls}-multiple ${componentCls}-selection-item`]: {
          background: token.multipleItemBg,
          border: `${unit(token.lineWidth)} ${token.lineType} ${token.multipleItemBorderColor}`
        }
      }
    }]
  };
};
export default genVariantsStyle;