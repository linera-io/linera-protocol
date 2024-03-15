import _slicedToArray from "@babel/runtime/helpers/esm/slicedToArray";
import useDelayState from "./useDelayState";

/**
 * Control the open state.
 * Will not close if activeElement is on the popup.
 */
export default function useOpen(open, defaultOpen) {
  var disabledList = arguments.length > 2 && arguments[2] !== undefined ? arguments[2] : [];
  var onOpenChange = arguments.length > 3 ? arguments[3] : undefined;
  var mergedOpen = disabledList.every(function (disabled) {
    return disabled;
  }) ? false : open;

  // Delay for handle the open state, in case fast shift from `open` -> `close` -> `open`
  // const [rafOpen, setRafOpen] = useLockState(open, defaultOpen || false, onOpenChange);
  var _useDelayState = useDelayState(mergedOpen, defaultOpen || false, onOpenChange),
    _useDelayState2 = _slicedToArray(_useDelayState, 2),
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