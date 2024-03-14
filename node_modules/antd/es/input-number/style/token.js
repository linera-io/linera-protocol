import { TinyColor } from '@ctrl/tinycolor';
import { initComponentToken } from '../../input/style/token';
export const prepareComponentToken = token => {
  var _a;
  const handleVisible = (_a = token.handleVisible) !== null && _a !== void 0 ? _a : 'auto';
  return Object.assign(Object.assign({}, initComponentToken(token)), {
    controlWidth: 90,
    handleWidth: token.controlHeightSM - token.lineWidth * 2,
    handleFontSize: token.fontSize / 2,
    handleVisible,
    handleActiveBg: token.colorFillAlter,
    handleBg: token.colorBgContainer,
    filledHandleBg: new TinyColor(token.colorFillSecondary).onBackground(token.colorBgContainer).toHexString(),
    handleHoverColor: token.colorPrimary,
    handleBorderColor: token.colorBorder,
    handleOpacity: handleVisible === true ? 1 : 0
  });
};