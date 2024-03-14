import _toConsumableArray from "@babel/runtime/helpers/esm/toConsumableArray";
import _slicedToArray from "@babel/runtime/helpers/esm/slicedToArray";
import _typeof from "@babel/runtime/helpers/esm/typeof";
import classNames from 'classnames';
import useMergedState from "rc-util/es/hooks/useMergedState";
import warning from "rc-util/es/warning";
import React from 'react';
import useItems from "./hooks/useItems";
import CollapsePanel from "./Panel";
function getActiveKeysArray(activeKey) {
  var currentActiveKey = activeKey;
  if (!Array.isArray(currentActiveKey)) {
    var activeKeyType = _typeof(currentActiveKey);
    currentActiveKey = activeKeyType === 'number' || activeKeyType === 'string' ? [currentActiveKey] : [];
  }
  return currentActiveKey.map(function (key) {
    return String(key);
  });
}
var Collapse = /*#__PURE__*/React.forwardRef(function (props, ref) {
  var _props$prefixCls = props.prefixCls,
    prefixCls = _props$prefixCls === void 0 ? 'rc-collapse' : _props$prefixCls,
    _props$destroyInactiv = props.destroyInactivePanel,
    destroyInactivePanel = _props$destroyInactiv === void 0 ? false : _props$destroyInactiv,
    style = props.style,
    accordion = props.accordion,
    className = props.className,
    children = props.children,
    collapsible = props.collapsible,
    openMotion = props.openMotion,
    expandIcon = props.expandIcon,
    rawActiveKey = props.activeKey,
    defaultActiveKey = props.defaultActiveKey,
    _onChange = props.onChange,
    items = props.items;
  var collapseClassName = classNames(prefixCls, className);
  var _useMergedState = useMergedState([], {
      value: rawActiveKey,
      onChange: function onChange(v) {
        return _onChange === null || _onChange === void 0 ? void 0 : _onChange(v);
      },
      defaultValue: defaultActiveKey,
      postState: getActiveKeysArray
    }),
    _useMergedState2 = _slicedToArray(_useMergedState, 2),
    activeKey = _useMergedState2[0],
    setActiveKey = _useMergedState2[1];
  var onItemClick = function onItemClick(key) {
    return setActiveKey(function () {
      if (accordion) {
        return activeKey[0] === key ? [] : [key];
      }
      var index = activeKey.indexOf(key);
      var isActive = index > -1;
      if (isActive) {
        return activeKey.filter(function (item) {
          return item !== key;
        });
      }
      return [].concat(_toConsumableArray(activeKey), [key]);
    });
  };

  // ======================== Children ========================
  warning(!children, '[rc-collapse] `children` will be removed in next major version. Please use `items` instead.');
  var mergedChildren = useItems(items, children, {
    prefixCls: prefixCls,
    accordion: accordion,
    openMotion: openMotion,
    expandIcon: expandIcon,
    collapsible: collapsible,
    destroyInactivePanel: destroyInactivePanel,
    onItemClick: onItemClick,
    activeKey: activeKey
  });

  // ======================== Render ========================
  return /*#__PURE__*/React.createElement("div", {
    ref: ref,
    className: collapseClassName,
    style: style,
    role: accordion ? 'tablist' : undefined
  }, mergedChildren);
});
export default Object.assign(Collapse, {
  /**
   * @deprecated use `items` instead, will be removed in `v4.0.0`
   */
  Panel: CollapsePanel
});