"use strict";

var _interopRequireWildcard = require("@babel/runtime/helpers/interopRequireWildcard").default;
var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault").default;
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = useFrameState;
var _raf = _interopRequireDefault(require("rc-util/lib/raf"));
var _react = _interopRequireWildcard(require("react"));
var React = _react;
function useFrameState(defaultValue) {
  const [value, setValue] = React.useState(defaultValue);
  const frameRef = (0, _react.useRef)(null);
  const batchRef = (0, _react.useRef)([]);
  const destroyRef = (0, _react.useRef)(false);
  React.useEffect(() => {
    destroyRef.current = false;
    return () => {
      destroyRef.current = true;
      _raf.default.cancel(frameRef.current);
      frameRef.current = null;
    };
  }, []);
  function setFrameValue(updater) {
    if (destroyRef.current) {
      return;
    }
    if (frameRef.current === null) {
      batchRef.current = [];
      frameRef.current = (0, _raf.default)(() => {
        frameRef.current = null;
        setValue(prevValue => {
          let current = prevValue;
          batchRef.current.forEach(func => {
            current = func(current);
          });
          return current;
        });
      });
    }
    batchRef.current.push(updater);
  }
  return [value, setFrameValue];
}