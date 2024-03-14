"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault").default;
var _interopRequireWildcard = require("@babel/runtime/helpers/interopRequireWildcard").default;
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = useSyncState;
var React = _interopRequireWildcard(require("react"));
var _useForceUpdate = _interopRequireDefault(require("./useForceUpdate"));
function useSyncState(initialValue) {
  const ref = React.useRef(initialValue);
  const forceUpdate = (0, _useForceUpdate.default)();
  return [() => ref.current, newValue => {
    ref.current = newValue;
    // re-render
    forceUpdate();
  }];
}