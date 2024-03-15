"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault").default;
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.prepareComponentToken = exports.default = void 0;
var _style = require("../../style");
var _motion = require("../../style/motion");
var _internal = require("../../theme/internal");
var _dragger = _interopRequireDefault(require("./dragger"));
var _list = _interopRequireDefault(require("./list"));
var _motion2 = _interopRequireDefault(require("./motion"));
var _picture = require("./picture");
var _rtl = _interopRequireDefault(require("./rtl"));
const genBaseStyle = token => {
  const {
    componentCls,
    colorTextDisabled
  } = token;
  return {
    [`${componentCls}-wrapper`]: Object.assign(Object.assign({}, (0, _style.resetComponent)(token)), {
      [componentCls]: {
        outline: 0,
        "input[type='file']": {
          cursor: 'pointer'
        }
      },
      [`${componentCls}-select`]: {
        display: 'inline-block'
      },
      [`${componentCls}-disabled`]: {
        color: colorTextDisabled,
        cursor: 'not-allowed'
      }
    })
  };
};
const prepareComponentToken = token => ({
  actionsColor: token.colorTextDescription
});
// ============================== Export ==============================
exports.prepareComponentToken = prepareComponentToken;
var _default = exports.default = (0, _internal.genStyleHooks)('Upload', token => {
  const {
    fontSizeHeading3,
    fontHeight,
    lineWidth,
    controlHeightLG,
    calc
  } = token;
  const uploadToken = (0, _internal.mergeToken)(token, {
    uploadThumbnailSize: calc(fontSizeHeading3).mul(2).equal(),
    uploadProgressOffset: calc(calc(fontHeight).div(2)).add(lineWidth).equal(),
    uploadPicCardSize: calc(controlHeightLG).mul(2.55).equal()
  });
  return [genBaseStyle(uploadToken), (0, _dragger.default)(uploadToken), (0, _picture.genPictureStyle)(uploadToken), (0, _picture.genPictureCardStyle)(uploadToken), (0, _list.default)(uploadToken), (0, _motion2.default)(uploadToken), (0, _rtl.default)(uploadToken), (0, _motion.genCollapseMotion)(uploadToken)];
}, prepareComponentToken);