import { unit } from '@ant-design/cssinjs';
const genPaginationStyle = token => {
  const {
    componentCls,
    antCls,
    margin
  } = token;
  return {
    [`${componentCls}-wrapper`]: {
      // ========================== Pagination ==========================
      [`${componentCls}-pagination${antCls}-pagination`]: {
        margin: `${unit(margin)} 0`
      },
      [`${componentCls}-pagination`]: {
        display: 'flex',
        flexWrap: 'wrap',
        rowGap: token.paddingXS,
        '> *': {
          flex: 'none'
        },
        '&-left': {
          justifyContent: 'flex-start'
        },
        '&-center': {
          justifyContent: 'center'
        },
        '&-right': {
          justifyContent: 'flex-end'
        }
      }
    }
  };
};
export default genPaginationStyle;