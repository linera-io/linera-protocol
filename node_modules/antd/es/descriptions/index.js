"use client";

var __rest = this && this.__rest || function (s, e) {
  var t = {};
  for (var p in s) if (Object.prototype.hasOwnProperty.call(s, p) && e.indexOf(p) < 0) t[p] = s[p];
  if (s != null && typeof Object.getOwnPropertySymbols === "function") for (var i = 0, p = Object.getOwnPropertySymbols(s); i < p.length; i++) {
    if (e.indexOf(p[i]) < 0 && Object.prototype.propertyIsEnumerable.call(s, p[i])) t[p[i]] = s[p[i]];
  }
  return t;
};
/* eslint-disable react/no-array-index-key */
import * as React from 'react';
import classNames from 'classnames';
import { matchScreen } from '../_util/responsiveObserver';
import { ConfigContext } from '../config-provider';
import useSize from '../config-provider/hooks/useSize';
import useBreakpoint from '../grid/hooks/useBreakpoint';
import DEFAULT_COLUMN_MAP from './constant';
import DescriptionsContext from './DescriptionsContext';
import useItems from './hooks/useItems';
import useRow from './hooks/useRow';
import DescriptionsItem from './Item';
import Row from './Row';
import useStyle from './style';
const Descriptions = props => {
  const {
      prefixCls: customizePrefixCls,
      title,
      extra,
      column,
      colon = true,
      bordered,
      layout,
      children,
      className,
      rootClassName,
      style,
      size: customizeSize,
      labelStyle,
      contentStyle,
      items
    } = props,
    restProps = __rest(props, ["prefixCls", "title", "extra", "column", "colon", "bordered", "layout", "children", "className", "rootClassName", "style", "size", "labelStyle", "contentStyle", "items"]);
  const {
    getPrefixCls,
    direction,
    descriptions
  } = React.useContext(ConfigContext);
  const prefixCls = getPrefixCls('descriptions', customizePrefixCls);
  const screens = useBreakpoint();
  // Column count
  const mergedColumn = React.useMemo(() => {
    var _a;
    if (typeof column === 'number') {
      return column;
    }
    return (_a = matchScreen(screens, Object.assign(Object.assign({}, DEFAULT_COLUMN_MAP), column))) !== null && _a !== void 0 ? _a : 3;
  }, [screens, column]);
  // Items with responsive
  const mergedItems = useItems(screens, items, children);
  const mergedSize = useSize(customizeSize);
  const rows = useRow(mergedColumn, mergedItems);
  const [wrapCSSVar, hashId, cssVarCls] = useStyle(prefixCls);
  // ======================== Render ========================
  const contextValue = React.useMemo(() => ({
    labelStyle,
    contentStyle
  }), [labelStyle, contentStyle]);
  return wrapCSSVar( /*#__PURE__*/React.createElement(DescriptionsContext.Provider, {
    value: contextValue
  }, /*#__PURE__*/React.createElement("div", Object.assign({
    className: classNames(prefixCls, descriptions === null || descriptions === void 0 ? void 0 : descriptions.className, {
      [`${prefixCls}-${mergedSize}`]: mergedSize && mergedSize !== 'default',
      [`${prefixCls}-bordered`]: !!bordered,
      [`${prefixCls}-rtl`]: direction === 'rtl'
    }, className, rootClassName, hashId, cssVarCls),
    style: Object.assign(Object.assign({}, descriptions === null || descriptions === void 0 ? void 0 : descriptions.style), style)
  }, restProps), (title || extra) && ( /*#__PURE__*/React.createElement("div", {
    className: `${prefixCls}-header`
  }, title && /*#__PURE__*/React.createElement("div", {
    className: `${prefixCls}-title`
  }, title), extra && /*#__PURE__*/React.createElement("div", {
    className: `${prefixCls}-extra`
  }, extra))), /*#__PURE__*/React.createElement("div", {
    className: `${prefixCls}-view`
  }, /*#__PURE__*/React.createElement("table", null, /*#__PURE__*/React.createElement("tbody", null, rows.map((row, index) => ( /*#__PURE__*/React.createElement(Row, {
    key: index,
    index: index,
    colon: colon,
    prefixCls: prefixCls,
    vertical: layout === 'vertical',
    bordered: bordered,
    row: row
  })))))))));
};
if (process.env.NODE_ENV !== 'production') {
  Descriptions.displayName = 'Descriptions';
}
export { DescriptionsContext };
Descriptions.Item = DescriptionsItem;
export default Descriptions;