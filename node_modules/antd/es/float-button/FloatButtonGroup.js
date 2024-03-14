"use client";

var __rest = this && this.__rest || function (s, e) {
  var t = {};
  for (var p in s) if (Object.prototype.hasOwnProperty.call(s, p) && e.indexOf(p) < 0) t[p] = s[p];
  if (s != null && typeof Object.getOwnPropertySymbols === "function") for (var i = 0, p = Object.getOwnPropertySymbols(s); i < p.length; i++) {
    if (e.indexOf(p[i]) < 0 && Object.prototype.propertyIsEnumerable.call(s, p[i])) t[p[i]] = s[p[i]];
  }
  return t;
};
import React, { memo, useCallback, useContext, useEffect } from 'react';
import CloseOutlined from "@ant-design/icons/es/icons/CloseOutlined";
import FileTextOutlined from "@ant-design/icons/es/icons/FileTextOutlined";
import classNames from 'classnames';
import CSSMotion from 'rc-motion';
import useMergedState from "rc-util/es/hooks/useMergedState";
import { devUseWarning } from '../_util/warning';
import { ConfigContext } from '../config-provider';
import { FloatButtonGroupProvider } from './context';
import FloatButton, { floatButtonPrefixCls } from './FloatButton';
import useStyle from './style';
import useCSSVarCls from '../config-provider/hooks/useCSSVarCls';
const FloatButtonGroup = props => {
  const {
      prefixCls: customizePrefixCls,
      className,
      style,
      shape = 'circle',
      type = 'default',
      icon = /*#__PURE__*/React.createElement(FileTextOutlined, null),
      closeIcon = /*#__PURE__*/React.createElement(CloseOutlined, null),
      description,
      trigger,
      children,
      onOpenChange,
      open: customOpen
    } = props,
    floatButtonProps = __rest(props, ["prefixCls", "className", "style", "shape", "type", "icon", "closeIcon", "description", "trigger", "children", "onOpenChange", "open"]);
  const {
    direction,
    getPrefixCls
  } = useContext(ConfigContext);
  const prefixCls = getPrefixCls(floatButtonPrefixCls, customizePrefixCls);
  const rootCls = useCSSVarCls(prefixCls);
  const [wrapCSSVar, hashId, cssVarCls] = useStyle(prefixCls, rootCls);
  const groupPrefixCls = `${prefixCls}-group`;
  const groupCls = classNames(groupPrefixCls, hashId, cssVarCls, rootCls, className, {
    [`${groupPrefixCls}-rtl`]: direction === 'rtl',
    [`${groupPrefixCls}-${shape}`]: shape,
    [`${groupPrefixCls}-${shape}-shadow`]: !trigger
  });
  const wrapperCls = classNames(hashId, `${groupPrefixCls}-wrap`);
  const [open, setOpen] = useMergedState(false, {
    value: customOpen
  });
  const floatButtonGroupRef = React.useRef(null);
  const floatButtonRef = React.useRef(null);
  const hoverAction = React.useMemo(() => {
    const hoverTypeAction = {
      onMouseEnter() {
        setOpen(true);
        onOpenChange === null || onOpenChange === void 0 ? void 0 : onOpenChange(true);
      },
      onMouseLeave() {
        setOpen(false);
        onOpenChange === null || onOpenChange === void 0 ? void 0 : onOpenChange(false);
      }
    };
    return trigger === 'hover' ? hoverTypeAction : {};
  }, [trigger]);
  const handleOpenChange = () => {
    setOpen(prevState => {
      onOpenChange === null || onOpenChange === void 0 ? void 0 : onOpenChange(!prevState);
      return !prevState;
    });
  };
  const onClick = useCallback(e => {
    var _a, _b;
    if ((_a = floatButtonGroupRef.current) === null || _a === void 0 ? void 0 : _a.contains(e.target)) {
      if ((_b = floatButtonRef.current) === null || _b === void 0 ? void 0 : _b.contains(e.target)) {
        handleOpenChange();
      }
      return;
    }
    setOpen(false);
    onOpenChange === null || onOpenChange === void 0 ? void 0 : onOpenChange(false);
  }, [trigger]);
  useEffect(() => {
    if (trigger === 'click') {
      document.addEventListener('click', onClick);
      return () => {
        document.removeEventListener('click', onClick);
      };
    }
  }, [trigger]);
  // =================== Warning =====================
  if (process.env.NODE_ENV !== 'production') {
    const warning = devUseWarning('FloatButton.Group');
    process.env.NODE_ENV !== "production" ? warning(!('open' in props) || !!trigger, 'usage', '`open` need to be used together with `trigger`') : void 0;
  }
  return wrapCSSVar( /*#__PURE__*/React.createElement(FloatButtonGroupProvider, {
    value: shape
  }, /*#__PURE__*/React.createElement("div", Object.assign({
    ref: floatButtonGroupRef,
    className: groupCls,
    style: style
  }, hoverAction), trigger && ['click', 'hover'].includes(trigger) ? ( /*#__PURE__*/React.createElement(React.Fragment, null, /*#__PURE__*/React.createElement(CSSMotion, {
    visible: open,
    motionName: `${groupPrefixCls}-wrap`
  }, _ref => {
    let {
      className: motionClassName
    } = _ref;
    return /*#__PURE__*/React.createElement("div", {
      className: classNames(motionClassName, wrapperCls)
    }, children);
  }), /*#__PURE__*/React.createElement(FloatButton, Object.assign({
    ref: floatButtonRef,
    type: type,
    shape: shape,
    icon: open ? closeIcon : icon,
    description: description,
    "aria-label": props['aria-label']
  }, floatButtonProps)))) : children)));
};
export default /*#__PURE__*/memo(FloatButtonGroup);