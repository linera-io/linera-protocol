import _objectWithoutProperties from "@babel/runtime/helpers/esm/objectWithoutProperties";
var _excluded = ["item"];
import warning from "rc-util/es/warning";

/**
 * `onClick` event return `info.item` which point to react node directly.
 * We should warning this since it will not work on FC.
 */
export function warnItemProp(_ref) {
  var item = _ref.item,
    restInfo = _objectWithoutProperties(_ref, _excluded);
  Object.defineProperty(restInfo, 'item', {
    get: function get() {
      warning(false, '`info.item` is deprecated since we will move to function component that not provides React Node instance in future.');
      return item;
    }
  });
  return restInfo;
}