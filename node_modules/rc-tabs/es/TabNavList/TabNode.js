import _defineProperty from "@babel/runtime/helpers/esm/defineProperty";
import classNames from 'classnames';
import KeyCode from "rc-util/es/KeyCode";
import * as React from 'react';
import { genDataNodeKey, getRemovable } from "../util";
var TabNode = function TabNode(props) {
  var prefixCls = props.prefixCls,
    id = props.id,
    active = props.active,
    _props$tab = props.tab,
    key = _props$tab.key,
    label = _props$tab.label,
    disabled = _props$tab.disabled,
    closeIcon = _props$tab.closeIcon,
    icon = _props$tab.icon,
    closable = props.closable,
    renderWrapper = props.renderWrapper,
    removeAriaLabel = props.removeAriaLabel,
    editable = props.editable,
    onClick = props.onClick,
    onFocus = props.onFocus,
    style = props.style;
  var tabPrefix = "".concat(prefixCls, "-tab");
  var removable = getRemovable(closable, closeIcon, editable, disabled);
  function onInternalClick(e) {
    if (disabled) {
      return;
    }
    onClick(e);
  }
  function onRemoveTab(event) {
    event.preventDefault();
    event.stopPropagation();
    editable.onEdit('remove', {
      key: key,
      event: event
    });
  }
  var labelNode = React.useMemo(function () {
    return icon && typeof label === 'string' ? /*#__PURE__*/React.createElement("span", null, label) : label;
  }, [label, icon]);
  var node = /*#__PURE__*/React.createElement("div", {
    key: key
    // ref={ref}
    ,
    "data-node-key": genDataNodeKey(key),
    className: classNames(tabPrefix, _defineProperty(_defineProperty(_defineProperty({}, "".concat(tabPrefix, "-with-remove"), removable), "".concat(tabPrefix, "-active"), active), "".concat(tabPrefix, "-disabled"), disabled)),
    style: style,
    onClick: onInternalClick
  }, /*#__PURE__*/React.createElement("div", {
    role: "tab",
    "aria-selected": active,
    id: id && "".concat(id, "-tab-").concat(key),
    className: "".concat(tabPrefix, "-btn"),
    "aria-controls": id && "".concat(id, "-panel-").concat(key),
    "aria-disabled": disabled,
    tabIndex: disabled ? null : 0,
    onClick: function onClick(e) {
      e.stopPropagation();
      onInternalClick(e);
    },
    onKeyDown: function onKeyDown(e) {
      if ([KeyCode.SPACE, KeyCode.ENTER].includes(e.which)) {
        e.preventDefault();
        onInternalClick(e);
      }
    },
    onFocus: onFocus
  }, icon && /*#__PURE__*/React.createElement("span", {
    className: "".concat(tabPrefix, "-icon")
  }, icon), label && labelNode), removable && /*#__PURE__*/React.createElement("button", {
    type: "button",
    "aria-label": removeAriaLabel || 'remove',
    tabIndex: 0,
    className: "".concat(tabPrefix, "-remove"),
    onClick: function onClick(e) {
      e.stopPropagation();
      onRemoveTab(e);
    }
  }, closeIcon || editable.removeIcon || '×'));
  return renderWrapper ? renderWrapper(node) : node;
};
export default TabNode;