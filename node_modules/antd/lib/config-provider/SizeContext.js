"use strict";
"use client";

var _interopRequireWildcard = require("@babel/runtime/helpers/interopRequireWildcard").default;
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = exports.SizeContextProvider = void 0;
var React = _interopRequireWildcard(require("react"));
const SizeContext = /*#__PURE__*/React.createContext(undefined);
const SizeContextProvider = _ref => {
  let {
    children,
    size
  } = _ref;
  const originSize = React.useContext(SizeContext);
  return /*#__PURE__*/React.createElement(SizeContext.Provider, {
    value: size || originSize
  }, children);
};
exports.SizeContextProvider = SizeContextProvider;
var _default = exports.default = SizeContext;