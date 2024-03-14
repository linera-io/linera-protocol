import _defineProperty from "@babel/runtime/helpers/esm/defineProperty";
import _slicedToArray from "@babel/runtime/helpers/esm/slicedToArray";
import classNames from 'classnames';
import Dropdown from 'rc-dropdown';
import Menu, { MenuItem } from 'rc-menu';
import KeyCode from "rc-util/es/KeyCode";
import * as React from 'react';
import { useEffect, useState } from 'react';
import { getRemovable } from "../util";
import AddButton from "./AddButton";
var OperationNode = /*#__PURE__*/React.forwardRef(function (props, ref) {
  var prefixCls = props.prefixCls,
    id = props.id,
    tabs = props.tabs,
    locale = props.locale,
    mobile = props.mobile,
    _props$moreIcon = props.moreIcon,
    moreIcon = _props$moreIcon === void 0 ? 'More' : _props$moreIcon,
    moreTransitionName = props.moreTransitionName,
    style = props.style,
    className = props.className,
    editable = props.editable,
    tabBarGutter = props.tabBarGutter,
    rtl = props.rtl,
    removeAriaLabel = props.removeAriaLabel,
    onTabClick = props.onTabClick,
    getPopupContainer = props.getPopupContainer,
    popupClassName = props.popupClassName;
  // ======================== Dropdown ========================
  var _useState = useState(false),
    _useState2 = _slicedToArray(_useState, 2),
    open = _useState2[0],
    setOpen = _useState2[1];
  var _useState3 = useState(null),
    _useState4 = _slicedToArray(_useState3, 2),
    selectedKey = _useState4[0],
    setSelectedKey = _useState4[1];
  var popupId = "".concat(id, "-more-popup");
  var dropdownPrefix = "".concat(prefixCls, "-dropdown");
  var selectedItemId = selectedKey !== null ? "".concat(popupId, "-").concat(selectedKey) : null;
  var dropdownAriaLabel = locale === null || locale === void 0 ? void 0 : locale.dropdownAriaLabel;
  function onRemoveTab(event, key) {
    event.preventDefault();
    event.stopPropagation();
    editable.onEdit('remove', {
      key: key,
      event: event
    });
  }
  var menu = /*#__PURE__*/React.createElement(Menu, {
    onClick: function onClick(_ref) {
      var key = _ref.key,
        domEvent = _ref.domEvent;
      onTabClick(key, domEvent);
      setOpen(false);
    },
    prefixCls: "".concat(dropdownPrefix, "-menu"),
    id: popupId,
    tabIndex: -1,
    role: "listbox",
    "aria-activedescendant": selectedItemId,
    selectedKeys: [selectedKey],
    "aria-label": dropdownAriaLabel !== undefined ? dropdownAriaLabel : 'expanded dropdown'
  }, tabs.map(function (tab) {
    var closable = tab.closable,
      disabled = tab.disabled,
      closeIcon = tab.closeIcon,
      key = tab.key,
      label = tab.label;
    var removable = getRemovable(closable, closeIcon, editable, disabled);
    return /*#__PURE__*/React.createElement(MenuItem, {
      key: key,
      id: "".concat(popupId, "-").concat(key),
      role: "option",
      "aria-controls": id && "".concat(id, "-panel-").concat(key),
      disabled: disabled
    }, /*#__PURE__*/React.createElement("span", null, label), removable && /*#__PURE__*/React.createElement("button", {
      type: "button",
      "aria-label": removeAriaLabel || 'remove',
      tabIndex: 0,
      className: "".concat(dropdownPrefix, "-menu-item-remove"),
      onClick: function onClick(e) {
        e.stopPropagation();
        onRemoveTab(e, key);
      }
    }, closeIcon || editable.removeIcon || '×'));
  }));
  function selectOffset(offset) {
    var enabledTabs = tabs.filter(function (tab) {
      return !tab.disabled;
    });
    var selectedIndex = enabledTabs.findIndex(function (tab) {
      return tab.key === selectedKey;
    }) || 0;
    var len = enabledTabs.length;
    for (var i = 0; i < len; i += 1) {
      selectedIndex = (selectedIndex + offset + len) % len;
      var tab = enabledTabs[selectedIndex];
      if (!tab.disabled) {
        setSelectedKey(tab.key);
        return;
      }
    }
  }
  function onKeyDown(e) {
    var which = e.which;
    if (!open) {
      if ([KeyCode.DOWN, KeyCode.SPACE, KeyCode.ENTER].includes(which)) {
        setOpen(true);
        e.preventDefault();
      }
      return;
    }
    switch (which) {
      case KeyCode.UP:
        selectOffset(-1);
        e.preventDefault();
        break;
      case KeyCode.DOWN:
        selectOffset(1);
        e.preventDefault();
        break;
      case KeyCode.ESC:
        setOpen(false);
        break;
      case KeyCode.SPACE:
      case KeyCode.ENTER:
        if (selectedKey !== null) {
          onTabClick(selectedKey, e);
        }
        break;
    }
  }

  // ========================= Effect =========================
  useEffect(function () {
    // We use query element here to avoid React strict warning
    var ele = document.getElementById(selectedItemId);
    if (ele && ele.scrollIntoView) {
      ele.scrollIntoView(false);
    }
  }, [selectedKey]);
  useEffect(function () {
    if (!open) {
      setSelectedKey(null);
    }
  }, [open]);

  // ========================= Render =========================
  var moreStyle = _defineProperty({}, rtl ? 'marginRight' : 'marginLeft', tabBarGutter);
  if (!tabs.length) {
    moreStyle.visibility = 'hidden';
    moreStyle.order = 1;
  }
  var overlayClassName = classNames(_defineProperty({}, "".concat(dropdownPrefix, "-rtl"), rtl));
  var moreNode = mobile ? null : /*#__PURE__*/React.createElement(Dropdown, {
    prefixCls: dropdownPrefix,
    overlay: menu,
    trigger: ['hover'],
    visible: tabs.length ? open : false,
    transitionName: moreTransitionName,
    onVisibleChange: setOpen,
    overlayClassName: classNames(overlayClassName, popupClassName),
    mouseEnterDelay: 0.1,
    mouseLeaveDelay: 0.1,
    getPopupContainer: getPopupContainer
  }, /*#__PURE__*/React.createElement("button", {
    type: "button",
    className: "".concat(prefixCls, "-nav-more"),
    style: moreStyle,
    tabIndex: -1,
    "aria-hidden": "true",
    "aria-haspopup": "listbox",
    "aria-controls": popupId,
    id: "".concat(id, "-more"),
    "aria-expanded": open,
    onKeyDown: onKeyDown
  }, moreIcon));
  return /*#__PURE__*/React.createElement("div", {
    className: classNames("".concat(prefixCls, "-nav-operations"), className),
    style: style,
    ref: ref
  }, moreNode, /*#__PURE__*/React.createElement(AddButton, {
    prefixCls: prefixCls,
    locale: locale,
    editable: editable
  }));
});
export default /*#__PURE__*/React.memo(OperationNode, function (_, next) {
  return (
    // https://github.com/ant-design/ant-design/issues/32544
    // We'd better remove syntactic sugar in `rc-menu` since this has perf issue
    next.tabMoving
  );
});