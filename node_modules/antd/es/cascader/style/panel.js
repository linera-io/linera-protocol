import { unit } from '@ant-design/cssinjs';
import { prepareComponentToken } from '.';
import { genComponentStyleHook } from '../../theme/internal';
import getColumnsStyle from './columns';
// ============================== Panel ===============================
const genPanelStyle = token => {
  const {
    componentCls
  } = token;
  return {
    [`${componentCls}-panel`]: [getColumnsStyle(token), {
      display: 'inline-flex',
      border: `${unit(token.lineWidth)} ${token.lineType} ${token.colorSplit}`,
      borderRadius: token.borderRadiusLG,
      overflowX: 'auto',
      maxWidth: '100%',
      [`${componentCls}-menus`]: {
        alignItems: 'stretch'
      },
      [`${componentCls}-menu`]: {
        height: 'auto'
      },
      '&-empty': {
        padding: token.paddingXXS
      }
    }]
  };
};
// ============================== Export ==============================
export default genComponentStyleHook(['Cascader', 'Panel'], token => genPanelStyle(token), prepareComponentToken);