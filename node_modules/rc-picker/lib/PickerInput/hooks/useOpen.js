"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault");
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = useOpen;
var _slicedToArray2 = _interopRequireDefault(require("@babel/runtime/helpers/slicedToArray"));
var _useDelayState3 = _interopRequireDefault(require("./useDelayState"));
/**
 * Control the open state.
 * Will not close if activeElement is on the popup.
 */
function useOpen(open, defaultOpen) {
  var disabledList = arguments.length > 2 && arguments[2] !== undefined ? arguments[2] : [];
  var onOpenChange = arguments.length > 3 ? arguments[3] : undefined;
  var mergedOpen = disabledList.every(function (disabled) {
    return disabled;
  }) ? false : open;

  // Delay for handle the open state, in case fast shift from `open` -> `close` -> `open`
  // const [rafOpen, setRafOpen] = useLockState(open, defaultOpen || false, onOpenChange);
  var _useDelayState = (0, _useDelayState3.default)(mergedOpen, defaultOpen || false, onOpenChange),
    _useDelayState2 = (0, _slicedToArray2.default)(_useDelayState, 2),
    rafOpen = _useDelayState2[0],
    setRafOpen = _useDelayState2[1];
  function setOpen(next) {
    var config = arguments.length > 1 && arguments[1] !== undefined ? arguments[1] : {};
    if (!config.inherit || rafOpen) {
      setRafOpen(next, config.force);
    }
  }
  return [rafOpen, setOpen];
}