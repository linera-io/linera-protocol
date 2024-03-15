"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault").default;
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = void 0;
var _slicedToArray2 = _interopRequireDefault(require("@babel/runtime/helpers/slicedToArray"));
var _react = require("react");
var _util = require("../util");
function hasValue(value) {
  return value !== undefined;
}
var useColorState = function useColorState(defaultStateValue, option) {
  var defaultValue = option.defaultValue,
    value = option.value;
  var _useState = (0, _react.useState)(function () {
      var mergeState;
      if (hasValue(value)) {
        mergeState = value;
      } else if (hasValue(defaultValue)) {
        mergeState = defaultValue;
      } else {
        mergeState = defaultStateValue;
      }
      return (0, _util.generateColor)(mergeState);
    }),
    _useState2 = (0, _slicedToArray2.default)(_useState, 2),
    colorValue = _useState2[0],
    setColorValue = _useState2[1];
  (0, _react.useEffect)(function () {
    if (value) {
      setColorValue((0, _util.generateColor)(value));
    }
  }, [value]);
  return [colorValue, setColorValue];
};
var _default = exports.default = useColorState;