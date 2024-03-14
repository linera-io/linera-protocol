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
import classNames from 'classnames';
import toArray from "rc-util/es/Children/toArray";
import { isPresetSize, isValidGapNumber } from '../_util/gapSize';
import { ConfigContext } from '../config-provider';
import Compact from './Compact';
import { SpaceContextProvider } from './context';
import Item from './Item';
import useStyle from './style';
export { SpaceContext } from './context';
const Space = /*#__PURE__*/React.forwardRef((props, ref) => {
  var _a, _b;
  const {
    getPrefixCls,
    space,
    direction: directionConfig
  } = React.useContext(ConfigContext);
  const {
      size = (space === null || space === void 0 ? void 0 : space.size) || 'small',
      align,
      className,
      rootClassName,
      children,
      direction = 'horizontal',
      prefixCls: customizePrefixCls,
      split,
      style,
      wrap = false,
      classNames: customClassNames,
      styles
    } = props,
    otherProps = __rest(props, ["size", "align", "className", "rootClassName", "children", "direction", "prefixCls", "split", "style", "wrap", "classNames", "styles"]);
  const [horizontalSize, verticalSize] = Array.isArray(size) ? size : [size, size];
  const isPresetVerticalSize = isPresetSize(verticalSize);
  const isPresetHorizontalSize = isPresetSize(horizontalSize);
  const isValidVerticalSize = isValidGapNumber(verticalSize);
  const isValidHorizontalSize = isValidGapNumber(horizontalSize);
  const childNodes = toArray(children, {
    keepEmpty: true
  });
  const mergedAlign = align === undefined && direction === 'horizontal' ? 'center' : align;
  const prefixCls = getPrefixCls('space', customizePrefixCls);
  const [wrapCSSVar, hashId, cssVarCls] = useStyle(prefixCls);
  const cls = classNames(prefixCls, space === null || space === void 0 ? void 0 : space.className, hashId, `${prefixCls}-${direction}`, {
    [`${prefixCls}-rtl`]: directionConfig === 'rtl',
    [`${prefixCls}-align-${mergedAlign}`]: mergedAlign,
    [`${prefixCls}-gap-row-${verticalSize}`]: isPresetVerticalSize,
    [`${prefixCls}-gap-col-${horizontalSize}`]: isPresetHorizontalSize
  }, className, rootClassName, cssVarCls);
  const itemClassName = classNames(`${prefixCls}-item`, (_a = customClassNames === null || customClassNames === void 0 ? void 0 : customClassNames.item) !== null && _a !== void 0 ? _a : (_b = space === null || space === void 0 ? void 0 : space.classNames) === null || _b === void 0 ? void 0 : _b.item);
  // Calculate latest one
  let latestIndex = 0;
  const nodes = childNodes.map((child, i) => {
    var _a, _b;
    if (child !== null && child !== undefined) {
      latestIndex = i;
    }
    const key = child && child.key || `${itemClassName}-${i}`;
    return /*#__PURE__*/React.createElement(Item, {
      className: itemClassName,
      key: key,
      index: i,
      split: split,
      style: (_a = styles === null || styles === void 0 ? void 0 : styles.item) !== null && _a !== void 0 ? _a : (_b = space === null || space === void 0 ? void 0 : space.styles) === null || _b === void 0 ? void 0 : _b.item
    }, child);
  });
  const spaceContext = React.useMemo(() => ({
    latestIndex
  }), [latestIndex]);
  // =========================== Render ===========================
  if (childNodes.length === 0) {
    return null;
  }
  const gapStyle = {};
  if (wrap) {
    gapStyle.flexWrap = 'wrap';
  }
  if (!isPresetHorizontalSize && isValidHorizontalSize) {
    gapStyle.columnGap = horizontalSize;
  }
  if (!isPresetVerticalSize && isValidVerticalSize) {
    gapStyle.rowGap = verticalSize;
  }
  return wrapCSSVar( /*#__PURE__*/React.createElement("div", Object.assign({
    ref: ref,
    className: cls,
    style: Object.assign(Object.assign(Object.assign({}, gapStyle), space === null || space === void 0 ? void 0 : space.style), style)
  }, otherProps), /*#__PURE__*/React.createElement(SpaceContextProvider, {
    value: spaceContext
  }, nodes)));
});
if (process.env.NODE_ENV !== 'production') {
  Space.displayName = 'Space';
}
const CompoundedSpace = Space;
CompoundedSpace.Compact = Compact;
export default CompoundedSpace;