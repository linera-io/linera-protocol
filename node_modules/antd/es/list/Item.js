"use client";

var __rest = this && this.__rest || function (s, e) {
  var t = {};
  for (var p in s) if (Object.prototype.hasOwnProperty.call(s, p) && e.indexOf(p) < 0) t[p] = s[p];
  if (s != null && typeof Object.getOwnPropertySymbols === "function") for (var i = 0, p = Object.getOwnPropertySymbols(s); i < p.length; i++) {
    if (e.indexOf(p[i]) < 0 && Object.prototype.propertyIsEnumerable.call(s, p[i])) t[p[i]] = s[p[i]];
  }
  return t;
};
import classNames from 'classnames';
import React, { Children, forwardRef, useContext } from 'react';
import { cloneElement } from '../_util/reactNode';
import { ConfigContext } from '../config-provider';
import { Col } from '../grid';
import { ListContext } from './context';
export const Meta = _a => {
  var {
      prefixCls: customizePrefixCls,
      className,
      avatar,
      title,
      description
    } = _a,
    others = __rest(_a, ["prefixCls", "className", "avatar", "title", "description"]);
  const {
    getPrefixCls
  } = useContext(ConfigContext);
  const prefixCls = getPrefixCls('list', customizePrefixCls);
  const classString = classNames(`${prefixCls}-item-meta`, className);
  const content = /*#__PURE__*/React.createElement("div", {
    className: `${prefixCls}-item-meta-content`
  }, title && /*#__PURE__*/React.createElement("h4", {
    className: `${prefixCls}-item-meta-title`
  }, title), description && /*#__PURE__*/React.createElement("div", {
    className: `${prefixCls}-item-meta-description`
  }, description));
  return /*#__PURE__*/React.createElement("div", Object.assign({}, others, {
    className: classString
  }), avatar && /*#__PURE__*/React.createElement("div", {
    className: `${prefixCls}-item-meta-avatar`
  }, avatar), (title || description) && content);
};
const InternalItem = (_a, ref) => {
  var {
      prefixCls: customizePrefixCls,
      children,
      actions,
      extra,
      className,
      colStyle
    } = _a,
    others = __rest(_a, ["prefixCls", "children", "actions", "extra", "className", "colStyle"]);
  const {
    grid,
    itemLayout
  } = useContext(ListContext);
  const {
    getPrefixCls
  } = useContext(ConfigContext);
  const isItemContainsTextNodeAndNotSingular = () => {
    let result;
    Children.forEach(children, element => {
      if (typeof element === 'string') {
        result = true;
      }
    });
    return result && Children.count(children) > 1;
  };
  const isFlexMode = () => {
    if (itemLayout === 'vertical') {
      return !!extra;
    }
    return !isItemContainsTextNodeAndNotSingular();
  };
  const prefixCls = getPrefixCls('list', customizePrefixCls);
  const actionsContent = actions && actions.length > 0 && ( /*#__PURE__*/React.createElement("ul", {
    className: `${prefixCls}-item-action`,
    key: "actions"
  }, actions.map((action, i) => (
  /*#__PURE__*/
  // eslint-disable-next-line react/no-array-index-key
  React.createElement("li", {
    key: `${prefixCls}-item-action-${i}`
  }, action, i !== actions.length - 1 && /*#__PURE__*/React.createElement("em", {
    className: `${prefixCls}-item-action-split`
  }))))));
  const Element = grid ? 'div' : 'li';
  const itemChildren = /*#__PURE__*/React.createElement(Element, Object.assign({}, others, !grid ? {
    ref
  } : {}, {
    className: classNames(`${prefixCls}-item`, {
      [`${prefixCls}-item-no-flex`]: !isFlexMode()
    }, className)
  }), itemLayout === 'vertical' && extra ? [/*#__PURE__*/React.createElement("div", {
    className: `${prefixCls}-item-main`,
    key: "content"
  }, children, actionsContent), /*#__PURE__*/React.createElement("div", {
    className: `${prefixCls}-item-extra`,
    key: "extra"
  }, extra)] : [children, actionsContent, cloneElement(extra, {
    key: 'extra'
  })]);
  return grid ? ( /*#__PURE__*/React.createElement(Col, {
    ref: ref,
    flex: 1,
    style: colStyle
  }, itemChildren)) : itemChildren;
};
const Item = /*#__PURE__*/forwardRef(InternalItem);
Item.Meta = Meta;
export default Item;