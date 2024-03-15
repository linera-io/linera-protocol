"use strict";
"use client";

var _interopRequireWildcard = require("@babel/runtime/helpers/interopRequireWildcard").default;
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = MotionWrapper;
var _rcMotion = require("rc-motion");
var React = _interopRequireWildcard(require("react"));
var _internal = require("../theme/internal");
function MotionWrapper(props) {
  const {
    children
  } = props;
  const [, token] = (0, _internal.useToken)();
  const {
    motion
  } = token;
  const needWrapMotionProviderRef = React.useRef(false);
  needWrapMotionProviderRef.current = needWrapMotionProviderRef.current || motion === false;
  if (needWrapMotionProviderRef.current) {
    return /*#__PURE__*/React.createElement(_rcMotion.Provider, {
      motion: motion
    }, children);
  }
  return children;
}