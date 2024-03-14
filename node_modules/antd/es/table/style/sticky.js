import { unit } from '@ant-design/cssinjs';
const genStickyStyle = token => {
  const {
    componentCls,
    opacityLoading,
    tableScrollThumbBg,
    tableScrollThumbBgHover,
    tableScrollThumbSize,
    tableScrollBg,
    zIndexTableSticky,
    stickyScrollBarBorderRadius,
    lineWidth,
    lineType,
    tableBorderColor
  } = token;
  const tableBorder = `${unit(lineWidth)} ${lineType} ${tableBorderColor}`;
  return {
    [`${componentCls}-wrapper`]: {
      [`${componentCls}-sticky`]: {
        '&-holder': {
          position: 'sticky',
          zIndex: zIndexTableSticky,
          background: token.colorBgContainer
        },
        '&-scroll': {
          position: 'sticky',
          bottom: 0,
          height: `${unit(tableScrollThumbSize)} !important`,
          zIndex: zIndexTableSticky,
          display: 'flex',
          alignItems: 'center',
          background: tableScrollBg,
          borderTop: tableBorder,
          opacity: opacityLoading,
          '&:hover': {
            transformOrigin: 'center bottom'
          },
          // fake scrollbar style of sticky
          '&-bar': {
            height: tableScrollThumbSize,
            backgroundColor: tableScrollThumbBg,
            borderRadius: stickyScrollBarBorderRadius,
            transition: `all ${token.motionDurationSlow}, transform none`,
            position: 'absolute',
            bottom: 0,
            '&:hover, &-active': {
              backgroundColor: tableScrollThumbBgHover
            }
          }
        }
      }
    }
  };
};
export default genStickyStyle;