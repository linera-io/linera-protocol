"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = exports.Variants = void 0;
var _react = require("react");
var _context = require("../context");
const Variants = exports.Variants = ['outlined', 'borderless', 'filled'];
/**
 * Compatible for legacy `bordered` prop.
 */
const useVariant = function (variant) {
  let legacyBordered = arguments.length > 1 && arguments[1] !== undefined ? arguments[1] : undefined;
  const ctxVariant = (0, _react.useContext)(_context.VariantContext);
  let mergedVariant;
  if (typeof variant !== 'undefined') {
    mergedVariant = variant;
  } else if (legacyBordered === false) {
    mergedVariant = 'borderless';
  } else {
    mergedVariant = ctxVariant !== null && ctxVariant !== void 0 ? ctxVariant : 'outlined';
  }
  const enableVariantCls = Variants.includes(mergedVariant);
  return [mergedVariant, enableVariantCls];
};
var _default = exports.default = useVariant;