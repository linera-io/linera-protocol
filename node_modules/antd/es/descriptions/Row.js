"use client";

import * as React from 'react';
import Cell from './Cell';
import DescriptionsContext from './DescriptionsContext';
function renderCells(items, _ref, _ref2) {
  let {
    colon,
    prefixCls,
    bordered
  } = _ref;
  let {
    component,
    type,
    showLabel,
    showContent,
    labelStyle: rootLabelStyle,
    contentStyle: rootContentStyle
  } = _ref2;
  return items.map((_ref3, index) => {
    let {
      label,
      children,
      prefixCls: itemPrefixCls = prefixCls,
      className,
      style,
      labelStyle,
      contentStyle,
      span = 1,
      key
    } = _ref3;
    if (typeof component === 'string') {
      return /*#__PURE__*/React.createElement(Cell, {
        key: `${type}-${key || index}`,
        className: className,
        style: style,
        labelStyle: Object.assign(Object.assign({}, rootLabelStyle), labelStyle),
        contentStyle: Object.assign(Object.assign({}, rootContentStyle), contentStyle),
        span: span,
        colon: colon,
        component: component,
        itemPrefixCls: itemPrefixCls,
        bordered: bordered,
        label: showLabel ? label : null,
        content: showContent ? children : null,
        type: type
      });
    }
    return [/*#__PURE__*/React.createElement(Cell, {
      key: `label-${key || index}`,
      className: className,
      style: Object.assign(Object.assign(Object.assign({}, rootLabelStyle), style), labelStyle),
      span: 1,
      colon: colon,
      component: component[0],
      itemPrefixCls: itemPrefixCls,
      bordered: bordered,
      label: label,
      type: "label"
    }), /*#__PURE__*/React.createElement(Cell, {
      key: `content-${key || index}`,
      className: className,
      style: Object.assign(Object.assign(Object.assign({}, rootContentStyle), style), contentStyle),
      span: span * 2 - 1,
      component: component[1],
      itemPrefixCls: itemPrefixCls,
      bordered: bordered,
      content: children,
      type: "content"
    })];
  });
}
const Row = props => {
  const descContext = React.useContext(DescriptionsContext);
  const {
    prefixCls,
    vertical,
    row,
    index,
    bordered
  } = props;
  if (vertical) {
    return /*#__PURE__*/React.createElement(React.Fragment, null, /*#__PURE__*/React.createElement("tr", {
      key: `label-${index}`,
      className: `${prefixCls}-row`
    }, renderCells(row, props, Object.assign({
      component: 'th',
      type: 'label',
      showLabel: true
    }, descContext))), /*#__PURE__*/React.createElement("tr", {
      key: `content-${index}`,
      className: `${prefixCls}-row`
    }, renderCells(row, props, Object.assign({
      component: 'td',
      type: 'content',
      showContent: true
    }, descContext))));
  }
  return /*#__PURE__*/React.createElement("tr", {
    key: index,
    className: `${prefixCls}-row`
  }, renderCells(row, props, Object.assign({
    component: bordered ? ['th', 'td'] : 'td',
    type: 'item',
    showLabel: true,
    showContent: true
  }, descContext)));
};
export default Row;