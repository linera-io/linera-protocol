import _defineProperty from "@babel/runtime/helpers/esm/defineProperty";
/* eslint react/prop-types: 0 */
import classNames from 'classnames';
import React from 'react';
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
  var cls = classNames(prefixCls, "".concat(prefixCls, "-").concat(page), (_classNames = {}, _defineProperty(_classNames, "".concat(prefixCls, "-active"), active), _defineProperty(_classNames, "".concat(prefixCls, "-disabled"), !page), _classNames), className);
  var handleClick = function handleClick() {
    onClick(page);
  };
  var handleKeyPress = function handleKeyPress(e) {
    onKeyPress(e, onClick, page);
  };
  var pager = itemRender(page, 'page', /*#__PURE__*/React.createElement("a", {
    rel: "nofollow"
  }, page));
  return pager ? /*#__PURE__*/React.createElement("li", {
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
export default Pager;