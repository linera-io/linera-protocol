"use client";

var __rest = this && this.__rest || function (s, e) {
  var t = {};
  for (var p in s) if (Object.prototype.hasOwnProperty.call(s, p) && e.indexOf(p) < 0) t[p] = s[p];
  if (s != null && typeof Object.getOwnPropertySymbols === "function") for (var i = 0, p = Object.getOwnPropertySymbols(s); i < p.length; i++) {
    if (e.indexOf(p[i]) < 0 && Object.prototype.propertyIsEnumerable.call(s, p[i])) t[p[i]] = s[p[i]];
  }
  return t;
};
import * as React from 'react';
import ExclamationCircleFilled from "@ant-design/icons/es/icons/ExclamationCircleFilled";
import classNames from 'classnames';
import useMergedState from "rc-util/es/hooks/useMergedState";
import KeyCode from "rc-util/es/KeyCode";
import omit from "rc-util/es/omit";
import { cloneElement } from '../_util/reactNode';
import { ConfigContext } from '../config-provider';
import Popover from '../popover';
import PurePanel, { Overlay } from './PurePanel';
import useStyle from './style';
const Popconfirm = /*#__PURE__*/React.forwardRef((props, ref) => {
  var _a, _b;
  const {
      prefixCls: customizePrefixCls,
      placement = 'top',
      trigger = 'click',
      okType = 'primary',
      icon = /*#__PURE__*/React.createElement(ExclamationCircleFilled, null),
      children,
      overlayClassName,
      onOpenChange,
      onVisibleChange
    } = props,
    restProps = __rest(props, ["prefixCls", "placement", "trigger", "okType", "icon", "children", "overlayClassName", "onOpenChange", "onVisibleChange"]);
  const {
    getPrefixCls
  } = React.useContext(ConfigContext);
  const [open, setOpen] = useMergedState(false, {
    value: (_a = props.open) !== null && _a !== void 0 ? _a : props.visible,
    defaultValue: (_b = props.defaultOpen) !== null && _b !== void 0 ? _b : props.defaultVisible
  });
  const settingOpen = (value, e) => {
    setOpen(value, true);
    onVisibleChange === null || onVisibleChange === void 0 ? void 0 : onVisibleChange(value);
    onOpenChange === null || onOpenChange === void 0 ? void 0 : onOpenChange(value, e);
  };
  const close = e => {
    settingOpen(false, e);
  };
  const onConfirm = e => {
    var _a;
    return (_a = props.onConfirm) === null || _a === void 0 ? void 0 : _a.call(this, e);
  };
  const onCancel = e => {
    var _a;
    settingOpen(false, e);
    (_a = props.onCancel) === null || _a === void 0 ? void 0 : _a.call(this, e);
  };
  const onKeyDown = e => {
    if (e.keyCode === KeyCode.ESC && open) {
      settingOpen(false, e);
    }
  };
  const onInternalOpenChange = value => {
    const {
      disabled = false
    } = props;
    if (disabled) {
      return;
    }
    settingOpen(value);
  };
  const prefixCls = getPrefixCls('popconfirm', customizePrefixCls);
  const overlayClassNames = classNames(prefixCls, overlayClassName);
  const [wrapCSSVar] = useStyle(prefixCls);
  return wrapCSSVar( /*#__PURE__*/React.createElement(Popover, Object.assign({}, omit(restProps, ['title']), {
    trigger: trigger,
    placement: placement,
    onOpenChange: onInternalOpenChange,
    open: open,
    ref: ref,
    overlayClassName: overlayClassNames,
    content: /*#__PURE__*/React.createElement(Overlay, Object.assign({
      okType: okType,
      icon: icon
    }, props, {
      prefixCls: prefixCls,
      close: close,
      onConfirm: onConfirm,
      onCancel: onCancel
    })),
    "data-popover-inject": true
  }), cloneElement(children, {
    onKeyDown: e => {
      var _a, _b;
      if ( /*#__PURE__*/React.isValidElement(children)) {
        (_b = children === null || children === void 0 ? void 0 : (_a = children.props).onKeyDown) === null || _b === void 0 ? void 0 : _b.call(_a, e);
      }
      onKeyDown(e);
    }
  })));
});
// We don't care debug panel
/* istanbul ignore next */
Popconfirm._InternalPanelDoNotUseOrYouWillBeFired = PurePanel;
if (process.env.NODE_ENV !== 'production') {
  Popconfirm.displayName = 'Popconfirm';
}
export default Popconfirm;