"use strict";
"use client";

var _interopRequireWildcard = require("@babel/runtime/helpers/interopRequireWildcard").default;
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = void 0;
var React = _interopRequireWildcard(require("react"));
var _warning = require("../_util/warning");
/**
 * Warning for ConfigProviderProps.
 * This will be empty function in production.
 */
const PropWarning = /*#__PURE__*/React.memo(_ref => {
  let {
    dropdownMatchSelectWidth
  } = _ref;
  const warning = (0, _warning.devUseWarning)('ConfigProvider');
  warning.deprecated(dropdownMatchSelectWidth === undefined, 'dropdownMatchSelectWidth', 'popupMatchSelectWidth');
  return null;
});
if (process.env.NODE_ENV !== 'production') {
  PropWarning.displayName = 'PropWarning';
}
var _default = exports.default = process.env.NODE_ENV !== 'production' ? PropWarning : () => null;