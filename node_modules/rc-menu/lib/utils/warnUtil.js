"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault");
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.warnItemProp = warnItemProp;
var _objectWithoutProperties2 = _interopRequireDefault(require("@babel/runtime/helpers/objectWithoutProperties"));
var _warning = _interopRequireDefault(require("rc-util/lib/warning"));
var _excluded = ["item"];
/**
 * `onClick` event return `info.item` which point to react node directly.
 * We should warning this since it will not work on FC.
 */
function warnItemProp(_ref) {
  var item = _ref.item,
    restInfo = (0, _objectWithoutProperties2.default)(_ref, _excluded);
  Object.defineProperty(restInfo, 'item', {
    get: function get() {
      (0, _warning.default)(false, '`info.item` is deprecated since we will move to function component that not provides React Node instance in future.');
      return item;
    }
  });
  return restInfo;
}