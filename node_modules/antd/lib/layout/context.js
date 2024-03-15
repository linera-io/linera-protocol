"use strict";

var _interopRequireWildcard = require("@babel/runtime/helpers/interopRequireWildcard").default;
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.LayoutContext = void 0;
var React = _interopRequireWildcard(require("react"));
const LayoutContext = exports.LayoutContext = /*#__PURE__*/React.createContext({
  siderHook: {
    addSider: () => null,
    removeSider: () => null
  }
});