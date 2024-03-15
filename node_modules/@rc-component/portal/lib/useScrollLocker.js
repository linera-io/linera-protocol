"use strict";

var _interopRequireWildcard = require("@babel/runtime/helpers/interopRequireWildcard").default;
var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault").default;
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = useScrollLocker;
var _slicedToArray2 = _interopRequireDefault(require("@babel/runtime/helpers/slicedToArray"));
var React = _interopRequireWildcard(require("react"));
var _dynamicCSS = require("rc-util/lib/Dom/dynamicCSS");
var _useLayoutEffect = _interopRequireDefault(require("rc-util/lib/hooks/useLayoutEffect"));
var _getScrollBarSize = require("rc-util/lib/getScrollBarSize");
var _util = require("./util");
var UNIQUE_ID = "rc-util-locker-".concat(Date.now());
var uuid = 0;
function useScrollLocker(lock) {
  var mergedLock = !!lock;
  var _React$useState = React.useState(function () {
      uuid += 1;
      return "".concat(UNIQUE_ID, "_").concat(uuid);
    }),
    _React$useState2 = (0, _slicedToArray2.default)(_React$useState, 1),
    id = _React$useState2[0];
  (0, _useLayoutEffect.default)(function () {
    if (mergedLock) {
      var scrollbarSize = (0, _getScrollBarSize.getTargetScrollBarSize)(document.body).width;
      var isOverflow = (0, _util.isBodyOverflowing)();
      (0, _dynamicCSS.updateCSS)("\nhtml body {\n  overflow-y: hidden;\n  ".concat(isOverflow ? "width: calc(100% - ".concat(scrollbarSize, "px);") : '', "\n}"), id);
    } else {
      (0, _dynamicCSS.removeCSS)(id);
    }
    return function () {
      (0, _dynamicCSS.removeCSS)(id);
    };
  }, [mergedLock, id]);
}