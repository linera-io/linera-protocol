"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault");
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.useLayoutState = useLayoutState;
exports.useTimeoutLock = useTimeoutLock;
var _slicedToArray2 = _interopRequireDefault(require("@babel/runtime/helpers/slicedToArray"));
var _react = require("react");
/**
 * Execute code before next frame but async
 */
function useLayoutState(defaultState) {
  var stateRef = (0, _react.useRef)(defaultState);
  var _useState = (0, _react.useState)({}),
    _useState2 = (0, _slicedToArray2.default)(_useState, 2),
    forceUpdate = _useState2[1];
  var lastPromiseRef = (0, _react.useRef)(null);
  var updateBatchRef = (0, _react.useRef)([]);
  function setFrameState(updater) {
    updateBatchRef.current.push(updater);
    var promise = Promise.resolve();
    lastPromiseRef.current = promise;
    promise.then(function () {
      if (lastPromiseRef.current === promise) {
        var prevBatch = updateBatchRef.current;
        var prevState = stateRef.current;
        updateBatchRef.current = [];
        prevBatch.forEach(function (batchUpdater) {
          stateRef.current = batchUpdater(stateRef.current);
        });
        lastPromiseRef.current = null;
        if (prevState !== stateRef.current) {
          forceUpdate({});
        }
      }
    });
  }
  (0, _react.useEffect)(function () {
    return function () {
      lastPromiseRef.current = null;
    };
  }, []);
  return [stateRef.current, setFrameState];
}

/** Lock frame, when frame pass reset the lock. */
function useTimeoutLock(defaultState) {
  var frameRef = (0, _react.useRef)(defaultState || null);
  var timeoutRef = (0, _react.useRef)();
  function cleanUp() {
    window.clearTimeout(timeoutRef.current);
  }
  function setState(newState) {
    frameRef.current = newState;
    cleanUp();
    timeoutRef.current = window.setTimeout(function () {
      frameRef.current = null;
      timeoutRef.current = undefined;
    }, 100);
  }
  function getState() {
    return frameRef.current;
  }
  (0, _react.useEffect)(function () {
    return cleanUp;
  }, []);
  return [setState, getState];
}