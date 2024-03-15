"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault");
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = useUpdate;
exports.useUpdateState = useUpdateState;
var _slicedToArray2 = _interopRequireDefault(require("@babel/runtime/helpers/slicedToArray"));
var _useLayoutEffect = require("rc-util/lib/hooks/useLayoutEffect");
var _react = require("react");
/**
 * Help to merge callback with `useLayoutEffect`.
 * One time will only trigger once.
 */
function useUpdate(callback) {
  var _useState = (0, _react.useState)(0),
    _useState2 = (0, _slicedToArray2.default)(_useState, 2),
    count = _useState2[0],
    setCount = _useState2[1];
  var effectRef = (0, _react.useRef)(0);
  var callbackRef = (0, _react.useRef)();
  callbackRef.current = callback;

  // Trigger on `useLayoutEffect`
  (0, _useLayoutEffect.useLayoutUpdateEffect)(function () {
    var _callbackRef$current;
    (_callbackRef$current = callbackRef.current) === null || _callbackRef$current === void 0 || _callbackRef$current.call(callbackRef);
  }, [count]);

  // Trigger to update count
  return function () {
    if (effectRef.current !== count) {
      return;
    }
    effectRef.current += 1;
    setCount(effectRef.current);
  };
}
function useUpdateState(defaultState) {
  var batchRef = (0, _react.useRef)([]);
  var _useState3 = (0, _react.useState)({}),
    _useState4 = (0, _slicedToArray2.default)(_useState3, 2),
    forceUpdate = _useState4[1];
  var state = (0, _react.useRef)(typeof defaultState === 'function' ? defaultState() : defaultState);
  var flushUpdate = useUpdate(function () {
    var current = state.current;
    batchRef.current.forEach(function (callback) {
      current = callback(current);
    });
    batchRef.current = [];
    state.current = current;
    forceUpdate({});
  });
  function updater(callback) {
    batchRef.current.push(callback);
    flushUpdate();
  }
  return [state.current, updater];
}