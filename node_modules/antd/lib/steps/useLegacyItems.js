"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault").default;
var _interopRequireWildcard = require("@babel/runtime/helpers/interopRequireWildcard").default;
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = useLegacyItems;
var React = _interopRequireWildcard(require("react"));
var _toArray = _interopRequireDefault(require("rc-util/lib/Children/toArray"));
var _warning = require("../_util/warning");
function filter(items) {
  return items.filter(item => item);
}
function useLegacyItems(items, children) {
  if (process.env.NODE_ENV === 'test') {
    const warning = (0, _warning.devUseWarning)('Menu');
    warning.deprecated(!children, 'Step', 'items');
  }
  if (items) {
    return items;
  }
  const childrenItems = (0, _toArray.default)(children).map(node => {
    if ( /*#__PURE__*/React.isValidElement(node)) {
      const {
        props
      } = node;
      const item = Object.assign({}, props);
      return item;
    }
    return null;
  });
  return filter(childrenItems);
}