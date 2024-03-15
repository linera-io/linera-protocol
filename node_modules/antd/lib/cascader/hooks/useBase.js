"use strict";

var _interopRequireWildcard = require("@babel/runtime/helpers/interopRequireWildcard").default;
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = useBase;
var React = _interopRequireWildcard(require("react"));
var _configProvider = require("../../config-provider");
function useBase(customizePrefixCls, direction) {
  const {
    getPrefixCls,
    direction: rootDirection,
    renderEmpty
  } = React.useContext(_configProvider.ConfigContext);
  const mergedDirection = direction || rootDirection;
  const prefixCls = getPrefixCls('select', customizePrefixCls);
  const cascaderPrefixCls = getPrefixCls('cascader', customizePrefixCls);
  return [prefixCls, cascaderPrefixCls, mergedDirection, renderEmpty];
}