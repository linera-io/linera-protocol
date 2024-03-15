"use strict";
"use client";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault").default;
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = void 0;
var _react = require("react");
var _useLayoutEffect = _interopRequireDefault(require("rc-util/lib/hooks/useLayoutEffect"));
var _useForceUpdate = _interopRequireDefault(require("../../_util/hooks/useForceUpdate"));
var _responsiveObserver = _interopRequireDefault(require("../../_util/responsiveObserver"));
function useBreakpoint() {
  let refreshOnChange = arguments.length > 0 && arguments[0] !== undefined ? arguments[0] : true;
  const screensRef = (0, _react.useRef)({});
  const forceUpdate = (0, _useForceUpdate.default)();
  const responsiveObserver = (0, _responsiveObserver.default)();
  (0, _useLayoutEffect.default)(() => {
    const token = responsiveObserver.subscribe(supportScreens => {
      screensRef.current = supportScreens;
      if (refreshOnChange) {
        forceUpdate();
      }
    });
    return () => responsiveObserver.unsubscribe(token);
  }, []);
  return screensRef.current;
}
var _default = exports.default = useBreakpoint;