"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.prepareComponentToken = void 0;
var _tinycolor = require("@ctrl/tinycolor");
var _token = require("../../input/style/token");
const prepareComponentToken = token => {
  var _a;
  const handleVisible = (_a = token.handleVisible) !== null && _a !== void 0 ? _a : 'auto';
  return Object.assign(Object.assign({}, (0, _token.initComponentToken)(token)), {
    controlWidth: 90,
    handleWidth: token.controlHeightSM - token.lineWidth * 2,
    handleFontSize: token.fontSize / 2,
    handleVisible,
    handleActiveBg: token.colorFillAlter,
    handleBg: token.colorBgContainer,
    filledHandleBg: new _tinycolor.TinyColor(token.colorFillSecondary).onBackground(token.colorBgContainer).toHexString(),
    handleHoverColor: token.colorPrimary,
    handleBorderColor: token.colorBorder,
    handleOpacity: handleVisible === true ? 1 : 0
  });
};
exports.prepareComponentToken = prepareComponentToken;