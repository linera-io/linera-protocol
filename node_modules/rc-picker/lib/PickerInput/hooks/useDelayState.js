"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault");
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = useDelayState;
var _slicedToArray2 = _interopRequireDefault(require("@babel/runtime/helpers/slicedToArray"));
var _rcUtil = require("rc-util");
var _raf = _interopRequireDefault(require("rc-util/lib/raf"));
var _react = _interopRequireDefault(require("react"));
/**
 * Will be `true` immediately for next effect.
 * But will be `false` for a delay of effect.
 */
function useDelayState(value, defaultValue, onChange) {
  var _useMergedState = (0, _rcUtil.useMergedState)(defaultValue, {
      value: value
    }),
    _useMergedState2 = (0, _slicedToArray2.default)(_useMergedState, 2),
    state = _useMergedState2[0],
    setState = _useMergedState2[1];
  var nextValueRef = _react.default.useRef(value);

  // ============================= Update =============================
  var rafRef = _react.default.useRef();
  var cancelRaf = function cancelRaf() {
    _raf.default.cancel(rafRef.current);
  };
  var doUpdate = (0, _rcUtil.useEvent)(function () {
    setState(nextValueRef.current);
    if (onChange && state !== nextValueRef.current) {
      onChange(nextValueRef.current);
    }
  });
  var updateValue = (0, _rcUtil.useEvent)(function (next, immediately) {
    cancelRaf();
    nextValueRef.current = next;
    if (next || immediately) {
      doUpdate();
    } else {
      rafRef.current = (0, _raf.default)(doUpdate);
    }
  });
  _react.default.useEffect(function () {
    return cancelRaf;
  }, []);
  return [state, updateValue];
}