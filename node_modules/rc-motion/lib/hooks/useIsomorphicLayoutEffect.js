"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault");
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = void 0;
var _canUseDom = _interopRequireDefault(require("rc-util/lib/Dom/canUseDom"));
var _react = require("react");
// It's safe to use `useLayoutEffect` but the warning is annoying
var useIsomorphicLayoutEffect = (0, _canUseDom.default)() ? _react.useLayoutEffect : _react.useEffect;
var _default = useIsomorphicLayoutEffect;
exports.default = _default;