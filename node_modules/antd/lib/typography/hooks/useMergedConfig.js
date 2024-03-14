"use strict";

var _interopRequireWildcard = require("@babel/runtime/helpers/interopRequireWildcard").default;
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = useMergedConfig;
var React = _interopRequireWildcard(require("react"));
function useMergedConfig(propConfig, templateConfig) {
  return React.useMemo(() => {
    const support = !!propConfig;
    return [support, Object.assign(Object.assign({}, templateConfig), support && typeof propConfig === 'object' ? propConfig : null)];
  }, [propConfig]);
}