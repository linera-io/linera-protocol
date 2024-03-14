import _toConsumableArray from "@babel/runtime/helpers/esm/toConsumableArray";
import raf from "rc-util/es/raf";
function throttleByAnimationFrame(fn) {
  let requestId;
  const later = args => () => {
    requestId = null;
    fn.apply(void 0, _toConsumableArray(args));
  };
  const throttled = function () {
    if (requestId == null) {
      for (var _len = arguments.length, args = new Array(_len), _key = 0; _key < _len; _key++) {
        args[_key] = arguments[_key];
      }
      requestId = raf(later(args));
    }
  };
  throttled.cancel = () => {
    raf.cancel(requestId);
    requestId = null;
  };
  return throttled;
}
export default throttleByAnimationFrame;