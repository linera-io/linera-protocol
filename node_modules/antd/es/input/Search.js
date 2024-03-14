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
import SearchOutlined from "@ant-design/icons/es/icons/SearchOutlined";
import classNames from 'classnames';
import { composeRef } from "rc-util/es/ref";
import { cloneElement } from '../_util/reactNode';
import Button from '../button';
import { ConfigContext } from '../config-provider';
import useSize from '../config-provider/hooks/useSize';
import { useCompactItemContext } from '../space/Compact';
import Input from './Input';
const Search = /*#__PURE__*/React.forwardRef((props, ref) => {
  const {
      prefixCls: customizePrefixCls,
      inputPrefixCls: customizeInputPrefixCls,
      className,
      size: customizeSize,
      suffix,
      enterButton = false,
      addonAfter,
      loading,
      disabled,
      onSearch: customOnSearch,
      onChange: customOnChange,
      onCompositionStart,
      onCompositionEnd
    } = props,
    restProps = __rest(props, ["prefixCls", "inputPrefixCls", "className", "size", "suffix", "enterButton", "addonAfter", "loading", "disabled", "onSearch", "onChange", "onCompositionStart", "onCompositionEnd"]);
  const {
    getPrefixCls,
    direction
  } = React.useContext(ConfigContext);
  const composedRef = React.useRef(false);
  const prefixCls = getPrefixCls('input-search', customizePrefixCls);
  const inputPrefixCls = getPrefixCls('input', customizeInputPrefixCls);
  const {
    compactSize
  } = useCompactItemContext(prefixCls, direction);
  const size = useSize(ctx => {
    var _a;
    return (_a = customizeSize !== null && customizeSize !== void 0 ? customizeSize : compactSize) !== null && _a !== void 0 ? _a : ctx;
  });
  const inputRef = React.useRef(null);
  const onChange = e => {
    if (e && e.target && e.type === 'click' && customOnSearch) {
      customOnSearch(e.target.value, e, {
        source: 'clear'
      });
    }
    if (customOnChange) {
      customOnChange(e);
    }
  };
  const onMouseDown = e => {
    var _a;
    if (document.activeElement === ((_a = inputRef.current) === null || _a === void 0 ? void 0 : _a.input)) {
      e.preventDefault();
    }
  };
  const onSearch = e => {
    var _a, _b;
    if (customOnSearch) {
      customOnSearch((_b = (_a = inputRef.current) === null || _a === void 0 ? void 0 : _a.input) === null || _b === void 0 ? void 0 : _b.value, e, {
        source: 'input'
      });
    }
  };
  const onPressEnter = e => {
    if (composedRef.current || loading) {
      return;
    }
    onSearch(e);
  };
  const searchIcon = typeof enterButton === 'boolean' ? /*#__PURE__*/React.createElement(SearchOutlined, null) : null;
  const btnClassName = `${prefixCls}-button`;
  let button;
  const enterButtonAsElement = enterButton || {};
  const isAntdButton = enterButtonAsElement.type && enterButtonAsElement.type.__ANT_BUTTON === true;
  if (isAntdButton || enterButtonAsElement.type === 'button') {
    button = cloneElement(enterButtonAsElement, Object.assign({
      onMouseDown,
      onClick: e => {
        var _a, _b;
        (_b = (_a = enterButtonAsElement === null || enterButtonAsElement === void 0 ? void 0 : enterButtonAsElement.props) === null || _a === void 0 ? void 0 : _a.onClick) === null || _b === void 0 ? void 0 : _b.call(_a, e);
        onSearch(e);
      },
      key: 'enterButton'
    }, isAntdButton ? {
      className: btnClassName,
      size
    } : {}));
  } else {
    button = /*#__PURE__*/React.createElement(Button, {
      className: btnClassName,
      type: enterButton ? 'primary' : undefined,
      size: size,
      disabled: disabled,
      key: "enterButton",
      onMouseDown: onMouseDown,
      onClick: onSearch,
      loading: loading,
      icon: searchIcon
    }, enterButton);
  }
  if (addonAfter) {
    button = [button, cloneElement(addonAfter, {
      key: 'addonAfter'
    })];
  }
  const cls = classNames(prefixCls, {
    [`${prefixCls}-rtl`]: direction === 'rtl',
    [`${prefixCls}-${size}`]: !!size,
    [`${prefixCls}-with-button`]: !!enterButton
  }, className);
  const handleOnCompositionStart = e => {
    composedRef.current = true;
    onCompositionStart === null || onCompositionStart === void 0 ? void 0 : onCompositionStart(e);
  };
  const handleOnCompositionEnd = e => {
    composedRef.current = false;
    onCompositionEnd === null || onCompositionEnd === void 0 ? void 0 : onCompositionEnd(e);
  };
  return /*#__PURE__*/React.createElement(Input, Object.assign({
    ref: composeRef(inputRef, ref),
    onPressEnter: onPressEnter
  }, restProps, {
    size: size,
    onCompositionStart: handleOnCompositionStart,
    onCompositionEnd: handleOnCompositionEnd,
    prefixCls: inputPrefixCls,
    addonAfter: button,
    suffix: suffix,
    onChange: onChange,
    className: cls,
    disabled: disabled
  }));
});
if (process.env.NODE_ENV !== 'production') {
  Search.displayName = 'Search';
}
export default Search;