"use strict";
"use client";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault").default;
var _interopRequireWildcard = require("@babel/runtime/helpers/interopRequireWildcard").default;
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = void 0;
var _react = _interopRequireWildcard(require("react"));
var _ = require(".");
var _empty = _interopRequireDefault(require("../empty"));
const DefaultRenderEmpty = props => {
  const {
    componentName
  } = props;
  const {
    getPrefixCls
  } = (0, _react.useContext)(_.ConfigContext);
  const prefix = getPrefixCls('empty');
  switch (componentName) {
    case 'Table':
    case 'List':
      return /*#__PURE__*/_react.default.createElement(_empty.default, {
        image: _empty.default.PRESENTED_IMAGE_SIMPLE
      });
    case 'Select':
    case 'TreeSelect':
    case 'Cascader':
    case 'Transfer':
    case 'Mentions':
      return /*#__PURE__*/_react.default.createElement(_empty.default, {
        image: _empty.default.PRESENTED_IMAGE_SIMPLE,
        className: `${prefix}-small`
      });
    /* istanbul ignore next */
    default:
      // Should never hit if we take all the component into consider.
      return /*#__PURE__*/_react.default.createElement(_empty.default, null);
  }
};
var _default = exports.default = DefaultRenderEmpty;