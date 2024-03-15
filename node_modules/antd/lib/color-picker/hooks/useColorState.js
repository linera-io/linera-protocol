"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = void 0;
var _react = require("react");
var _util = require("../util");
function hasValue(value) {
  return value !== undefined;
}
const useColorState = (defaultStateValue, option) => {
  const {
    defaultValue,
    value
  } = option;
  const [colorValue, setColorValue] = (0, _react.useState)(() => {
    let mergeState;
    if (hasValue(value)) {
      mergeState = value;
    } else if (hasValue(defaultValue)) {
      mergeState = defaultValue;
    } else {
      mergeState = defaultStateValue;
    }
    return (0, _util.generateColor)(mergeState || '');
  });
  (0, _react.useEffect)(() => {
    if (value) {
      setColorValue((0, _util.generateColor)(value));
    }
  }, [value]);
  return [colorValue, setColorValue];
};
var _default = exports.default = useColorState;