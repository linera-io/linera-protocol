"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = void 0;
var _internal = require("../../theme/internal");
/**
 * This hook is only for cssVar to add root className for components.
 * If root ClassName is needed, this hook could be refactored with `-root`
 * @param prefixCls
 */
const useCSSVarCls = prefixCls => {
  const [,,,, cssVar] = (0, _internal.useToken)();
  return cssVar ? `${prefixCls}-css-var` : '';
};
var _default = exports.default = useCSSVarCls;