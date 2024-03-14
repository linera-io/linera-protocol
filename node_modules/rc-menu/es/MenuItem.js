import _defineProperty from "@babel/runtime/helpers/esm/defineProperty";
import _objectSpread from "@babel/runtime/helpers/esm/objectSpread2";
import _toConsumableArray from "@babel/runtime/helpers/esm/toConsumableArray";
import _extends from "@babel/runtime/helpers/esm/extends";
import _objectWithoutProperties from "@babel/runtime/helpers/esm/objectWithoutProperties";
import _classCallCheck from "@babel/runtime/helpers/esm/classCallCheck";
import _createClass from "@babel/runtime/helpers/esm/createClass";
import _inherits from "@babel/runtime/helpers/esm/inherits";
import _createSuper from "@babel/runtime/helpers/esm/createSuper";
var _excluded = ["title", "attribute", "elementRef"],
  _excluded2 = ["style", "className", "eventKey", "warnKey", "disabled", "itemIcon", "children", "role", "onMouseEnter", "onMouseLeave", "onClick", "onKeyDown", "onFocus"],
  _excluded3 = ["active"];
import classNames from 'classnames';
import Overflow from 'rc-overflow';
import KeyCode from "rc-util/es/KeyCode";
import omit from "rc-util/es/omit";
import { useComposeRef } from "rc-util/es/ref";
import warning from "rc-util/es/warning";
import * as React from 'react';
import { useMenuId } from "./context/IdContext";
import { MenuContext } from "./context/MenuContext";
import { useFullPath, useMeasure } from "./context/PathContext";
import PrivateContext from "./context/PrivateContext";
import useActive from "./hooks/useActive";
import useDirectionStyle from "./hooks/useDirectionStyle";
import Icon from "./Icon";
import { warnItemProp } from "./utils/warnUtil";
// Since Menu event provide the `info.item` which point to the MenuItem node instance.
// We have to use class component here.
// This should be removed from doc & api in future.
var LegacyMenuItem = /*#__PURE__*/function (_React$Component) {
  _inherits(LegacyMenuItem, _React$Component);
  var _super = _createSuper(LegacyMenuItem);
  function LegacyMenuItem() {
    _classCallCheck(this, LegacyMenuItem);
    return _super.apply(this, arguments);
  }
  _createClass(LegacyMenuItem, [{
    key: "render",
    value: function render() {
      var _this$props = this.props,
        title = _this$props.title,
        attribute = _this$props.attribute,
        elementRef = _this$props.elementRef,
        restProps = _objectWithoutProperties(_this$props, _excluded);

      // Here the props are eventually passed to the DOM element.
      // React does not recognize non-standard attributes.
      // Therefore, remove the props that is not used here.
      // ref: https://github.com/ant-design/ant-design/issues/41395
      var passedProps = omit(restProps, ['eventKey', 'popupClassName', 'popupOffset', 'onTitleClick']);
      warning(!attribute, '`attribute` of Menu.Item is deprecated. Please pass attribute directly.');
      return /*#__PURE__*/React.createElement(Overflow.Item, _extends({}, attribute, {
        title: typeof title === 'string' ? title : undefined
      }, passedProps, {
        ref: elementRef
      }));
    }
  }]);
  return LegacyMenuItem;
}(React.Component);
/**
 * Real Menu Item component
 */
var InternalMenuItem = /*#__PURE__*/React.forwardRef(function (props, ref) {
  var style = props.style,
    className = props.className,
    eventKey = props.eventKey,
    warnKey = props.warnKey,
    disabled = props.disabled,
    itemIcon = props.itemIcon,
    children = props.children,
    role = props.role,
    onMouseEnter = props.onMouseEnter,
    onMouseLeave = props.onMouseLeave,
    onClick = props.onClick,
    onKeyDown = props.onKeyDown,
    onFocus = props.onFocus,
    restProps = _objectWithoutProperties(props, _excluded2);
  var domDataId = useMenuId(eventKey);
  var _React$useContext = React.useContext(MenuContext),
    prefixCls = _React$useContext.prefixCls,
    onItemClick = _React$useContext.onItemClick,
    contextDisabled = _React$useContext.disabled,
    overflowDisabled = _React$useContext.overflowDisabled,
    contextItemIcon = _React$useContext.itemIcon,
    selectedKeys = _React$useContext.selectedKeys,
    onActive = _React$useContext.onActive;
  var _React$useContext2 = React.useContext(PrivateContext),
    _internalRenderMenuItem = _React$useContext2._internalRenderMenuItem;
  var itemCls = "".concat(prefixCls, "-item");
  var legacyMenuItemRef = React.useRef();
  var elementRef = React.useRef();
  var mergedDisabled = contextDisabled || disabled;
  var mergedEleRef = useComposeRef(ref, elementRef);
  var connectedKeys = useFullPath(eventKey);

  // ================================ Warn ================================
  if (process.env.NODE_ENV !== 'production' && warnKey) {
    warning(false, 'MenuItem should not leave undefined `key`.');
  }

  // ============================= Info =============================
  var getEventInfo = function getEventInfo(e) {
    return {
      key: eventKey,
      // Note: For legacy code is reversed which not like other antd component
      keyPath: _toConsumableArray(connectedKeys).reverse(),
      item: legacyMenuItemRef.current,
      domEvent: e
    };
  };

  // ============================= Icon =============================
  var mergedItemIcon = itemIcon || contextItemIcon;

  // ============================ Active ============================
  var _useActive = useActive(eventKey, mergedDisabled, onMouseEnter, onMouseLeave),
    active = _useActive.active,
    activeProps = _objectWithoutProperties(_useActive, _excluded3);

  // ============================ Select ============================
  var selected = selectedKeys.includes(eventKey);

  // ======================== DirectionStyle ========================
  var directionStyle = useDirectionStyle(connectedKeys.length);

  // ============================ Events ============================
  var onInternalClick = function onInternalClick(e) {
    if (mergedDisabled) {
      return;
    }
    var info = getEventInfo(e);
    onClick === null || onClick === void 0 || onClick(warnItemProp(info));
    onItemClick(info);
  };
  var onInternalKeyDown = function onInternalKeyDown(e) {
    onKeyDown === null || onKeyDown === void 0 || onKeyDown(e);
    if (e.which === KeyCode.ENTER) {
      var info = getEventInfo(e);

      // Legacy. Key will also trigger click event
      onClick === null || onClick === void 0 || onClick(warnItemProp(info));
      onItemClick(info);
    }
  };

  /**
   * Used for accessibility. Helper will focus element without key board.
   * We should manually trigger an active
   */
  var onInternalFocus = function onInternalFocus(e) {
    onActive(eventKey);
    onFocus === null || onFocus === void 0 || onFocus(e);
  };

  // ============================ Render ============================
  var optionRoleProps = {};
  if (props.role === 'option') {
    optionRoleProps['aria-selected'] = selected;
  }
  var renderNode = /*#__PURE__*/React.createElement(LegacyMenuItem, _extends({
    ref: legacyMenuItemRef,
    elementRef: mergedEleRef,
    role: role === null ? 'none' : role || 'menuitem',
    tabIndex: disabled ? null : -1,
    "data-menu-id": overflowDisabled && domDataId ? null : domDataId
  }, restProps, activeProps, optionRoleProps, {
    component: "li",
    "aria-disabled": disabled,
    style: _objectSpread(_objectSpread({}, directionStyle), style),
    className: classNames(itemCls, _defineProperty(_defineProperty(_defineProperty({}, "".concat(itemCls, "-active"), active), "".concat(itemCls, "-selected"), selected), "".concat(itemCls, "-disabled"), mergedDisabled), className),
    onClick: onInternalClick,
    onKeyDown: onInternalKeyDown,
    onFocus: onInternalFocus
  }), children, /*#__PURE__*/React.createElement(Icon, {
    props: _objectSpread(_objectSpread({}, props), {}, {
      isSelected: selected
    }),
    icon: mergedItemIcon
  }));
  if (_internalRenderMenuItem) {
    renderNode = _internalRenderMenuItem(renderNode, props, {
      selected: selected
    });
  }
  return renderNode;
});
function MenuItem(props, ref) {
  var eventKey = props.eventKey;

  // ==================== Record KeyPath ====================
  var measure = useMeasure();
  var connectedKeyPath = useFullPath(eventKey);

  // eslint-disable-next-line consistent-return
  React.useEffect(function () {
    if (measure) {
      measure.registerPath(eventKey, connectedKeyPath);
      return function () {
        measure.unregisterPath(eventKey, connectedKeyPath);
      };
    }
  }, [connectedKeyPath]);
  if (measure) {
    return null;
  }

  // ======================== Render ========================
  return /*#__PURE__*/React.createElement(InternalMenuItem, _extends({}, props, {
    ref: ref
  }));
}
export default /*#__PURE__*/React.forwardRef(MenuItem);