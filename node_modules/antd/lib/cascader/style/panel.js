"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault").default;
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = void 0;
var _cssinjs = require("@ant-design/cssinjs");
var _ = require(".");
var _internal = require("../../theme/internal");
var _columns = _interopRequireDefault(require("./columns"));
// ============================== Panel ===============================
const genPanelStyle = token => {
  const {
    componentCls
  } = token;
  return {
    [`${componentCls}-panel`]: [(0, _columns.default)(token), {
      display: 'inline-flex',
      border: `${(0, _cssinjs.unit)(token.lineWidth)} ${token.lineType} ${token.colorSplit}`,
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
var _default = exports.default = (0, _internal.genComponentStyleHook)(['Cascader', 'Panel'], token => genPanelStyle(token), _.prepareComponentToken);