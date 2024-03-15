"use strict";
"use client";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault").default;
var _interopRequireWildcard = require("@babel/runtime/helpers/interopRequireWildcard").default;
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = void 0;
var React = _interopRequireWildcard(require("react"));
var _configProvider = require("../config-provider");
var _context = require("./context");
var _radio = _interopRequireDefault(require("./radio"));
var __rest = void 0 && (void 0).__rest || function (s, e) {
  var t = {};
  for (var p in s) if (Object.prototype.hasOwnProperty.call(s, p) && e.indexOf(p) < 0) t[p] = s[p];
  if (s != null && typeof Object.getOwnPropertySymbols === "function") for (var i = 0, p = Object.getOwnPropertySymbols(s); i < p.length; i++) {
    if (e.indexOf(p[i]) < 0 && Object.prototype.propertyIsEnumerable.call(s, p[i])) t[p[i]] = s[p[i]];
  }
  return t;
};
const RadioButton = (props, ref) => {
  const {
    getPrefixCls
  } = React.useContext(_configProvider.ConfigContext);
  const {
      prefixCls: customizePrefixCls
    } = props,
    radioProps = __rest(props, ["prefixCls"]);
  const prefixCls = getPrefixCls('radio', customizePrefixCls);
  return /*#__PURE__*/React.createElement(_context.RadioOptionTypeContextProvider, {
    value: "button"
  }, /*#__PURE__*/React.createElement(_radio.default, Object.assign({
    prefixCls: prefixCls
  }, radioProps, {
    type: "radio",
    ref: ref
  })));
};
var _default = exports.default = /*#__PURE__*/React.forwardRef(RadioButton);