"use client";

var __rest = this && this.__rest || function (s, e) {
  var t = {};
  for (var p in s) if (Object.prototype.hasOwnProperty.call(s, p) && e.indexOf(p) < 0) t[p] = s[p];
  if (s != null && typeof Object.getOwnPropertySymbols === "function") for (var i = 0, p = Object.getOwnPropertySymbols(s); i < p.length; i++) {
    if (e.indexOf(p[i]) < 0 && Object.prototype.propertyIsEnumerable.call(s, p[i])) t[p[i]] = s[p[i]];
  }
  return t;
};
import DoubleLeftOutlined from "@ant-design/icons/es/icons/DoubleLeftOutlined";
import DoubleRightOutlined from "@ant-design/icons/es/icons/DoubleRightOutlined";
import LeftOutlined from "@ant-design/icons/es/icons/LeftOutlined";
import RightOutlined from "@ant-design/icons/es/icons/RightOutlined";
import classNames from 'classnames';
import RcPagination from 'rc-pagination';
import enUS from "rc-pagination/es/locale/en_US";
import * as React from 'react';
import { ConfigContext } from '../config-provider';
import useSize from '../config-provider/hooks/useSize';
import useBreakpoint from '../grid/hooks/useBreakpoint';
import { useLocale } from '../locale';
import { MiddleSelect, MiniSelect } from './Select';
import useStyle from './style';
import { useToken } from '../theme/internal';
import BorderedStyle from './style/bordered';
const Pagination = props => {
  const {
      prefixCls: customizePrefixCls,
      selectPrefixCls: customizeSelectPrefixCls,
      className,
      rootClassName,
      style,
      size: customizeSize,
      locale: customLocale,
      selectComponentClass,
      responsive,
      showSizeChanger
    } = props,
    restProps = __rest(props, ["prefixCls", "selectPrefixCls", "className", "rootClassName", "style", "size", "locale", "selectComponentClass", "responsive", "showSizeChanger"]);
  const {
    xs
  } = useBreakpoint(responsive);
  const [, token] = useToken();
  const {
    getPrefixCls,
    direction,
    pagination = {}
  } = React.useContext(ConfigContext);
  const prefixCls = getPrefixCls('pagination', customizePrefixCls);
  // Style
  const [wrapCSSVar, hashId, cssVarCls] = useStyle(prefixCls);
  const mergedShowSizeChanger = showSizeChanger !== null && showSizeChanger !== void 0 ? showSizeChanger : pagination.showSizeChanger;
  const iconsProps = React.useMemo(() => {
    const ellipsis = /*#__PURE__*/React.createElement("span", {
      className: `${prefixCls}-item-ellipsis`
    }, "\u2022\u2022\u2022");
    const prevIcon = /*#__PURE__*/React.createElement("button", {
      className: `${prefixCls}-item-link`,
      type: "button",
      tabIndex: -1
    }, direction === 'rtl' ? /*#__PURE__*/React.createElement(RightOutlined, null) : /*#__PURE__*/React.createElement(LeftOutlined, null));
    const nextIcon = /*#__PURE__*/React.createElement("button", {
      className: `${prefixCls}-item-link`,
      type: "button",
      tabIndex: -1
    }, direction === 'rtl' ? /*#__PURE__*/React.createElement(LeftOutlined, null) : /*#__PURE__*/React.createElement(RightOutlined, null));
    const jumpPrevIcon = /*#__PURE__*/React.createElement("a", {
      className: `${prefixCls}-item-link`
    }, /*#__PURE__*/React.createElement("div", {
      className: `${prefixCls}-item-container`
    }, direction === 'rtl' ? ( /*#__PURE__*/React.createElement(DoubleRightOutlined, {
      className: `${prefixCls}-item-link-icon`
    })) : ( /*#__PURE__*/React.createElement(DoubleLeftOutlined, {
      className: `${prefixCls}-item-link-icon`
    })), ellipsis));
    const jumpNextIcon = /*#__PURE__*/React.createElement("a", {
      className: `${prefixCls}-item-link`
    }, /*#__PURE__*/React.createElement("div", {
      className: `${prefixCls}-item-container`
    }, direction === 'rtl' ? ( /*#__PURE__*/React.createElement(DoubleLeftOutlined, {
      className: `${prefixCls}-item-link-icon`
    })) : ( /*#__PURE__*/React.createElement(DoubleRightOutlined, {
      className: `${prefixCls}-item-link-icon`
    })), ellipsis));
    return {
      prevIcon,
      nextIcon,
      jumpPrevIcon,
      jumpNextIcon
    };
  }, [direction, prefixCls]);
  const [contextLocale] = useLocale('Pagination', enUS);
  const locale = Object.assign(Object.assign({}, contextLocale), customLocale);
  const mergedSize = useSize(customizeSize);
  const isSmall = mergedSize === 'small' || !!(xs && !mergedSize && responsive);
  const selectPrefixCls = getPrefixCls('select', customizeSelectPrefixCls);
  const extendedClassName = classNames({
    [`${prefixCls}-mini`]: isSmall,
    [`${prefixCls}-rtl`]: direction === 'rtl',
    [`${prefixCls}-bordered`]: token.wireframe
  }, pagination === null || pagination === void 0 ? void 0 : pagination.className, className, rootClassName, hashId, cssVarCls);
  const mergedStyle = Object.assign(Object.assign({}, pagination === null || pagination === void 0 ? void 0 : pagination.style), style);
  return wrapCSSVar( /*#__PURE__*/React.createElement(React.Fragment, null, token.wireframe && /*#__PURE__*/React.createElement(BorderedStyle, {
    prefixCls: prefixCls
  }), /*#__PURE__*/React.createElement(RcPagination, Object.assign({}, iconsProps, restProps, {
    style: mergedStyle,
    prefixCls: prefixCls,
    selectPrefixCls: selectPrefixCls,
    className: extendedClassName,
    selectComponentClass: selectComponentClass || (isSmall ? MiniSelect : MiddleSelect),
    locale: locale,
    showSizeChanger: mergedShowSizeChanger
  }))));
};
if (process.env.NODE_ENV !== 'production') {
  Pagination.displayName = 'Pagination';
}
export default Pagination;