"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault");
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = void 0;
var _defineProperty2 = _interopRequireDefault(require("@babel/runtime/helpers/defineProperty"));
var _classnames = _interopRequireDefault(require("classnames"));
var _react = _interopRequireDefault(require("react"));
/* eslint react/prop-types: 0 */

var Pager = function Pager(props) {
  var _classNames;
  var rootPrefixCls = props.rootPrefixCls,
    page = props.page,
    active = props.active,
    className = props.className,
    showTitle = props.showTitle,
    onClick = props.onClick,
    onKeyPress = props.onKeyPress,
    itemRender = props.itemRender;
  var prefixCls = "".concat(rootPrefixCls, "-item");
  var cls = (0, _classnames.default)(prefixCls, "".concat(prefixCls, "-").concat(page), (_classNames = {}, (0, _defineProperty2.default)(_classNames, "".concat(prefixCls, "-active"), active), (0, _defineProperty2.default)(_classNames, "".concat(prefixCls, "-disabled"), !page), _classNames), className);
  var handleClick = function handleClick() {
    onClick(page);
  };
  var handleKeyPress = function handleKeyPress(e) {
    onKeyPress(e, onClick, page);
  };
  var pager = itemRender(page, 'page', /*#__PURE__*/_react.default.createElement("a", {
    rel: "nofollow"
  }, page));
  return pager ? /*#__PURE__*/_react.default.createElement("li", {
    title: showTitle ? String(page) : null,
    className: cls,
    onClick: handleClick,
    onKeyDown: handleKeyPress,
    tabIndex: 0
  }, pager) : null;
};
if (process.env.NODE_ENV !== 'production') {
  Pager.displayName = 'Pager';
}
var _default = exports.default = Pager;