import _extends from "@babel/runtime/helpers/esm/extends";
import _typeof from "@babel/runtime/helpers/esm/typeof";
import _objectSpread from "@babel/runtime/helpers/esm/objectSpread2";
import _defineProperty from "@babel/runtime/helpers/esm/defineProperty";
import _slicedToArray from "@babel/runtime/helpers/esm/slicedToArray";
import { useContext } from '@rc-component/context';
import classNames from 'classnames';
import * as React from 'react';
import TableContext from "../context/TableContext";
import devRenderTimes from "../hooks/useRenderTimes";
import useCellRender from "./useCellRender";
import useHoverState from "./useHoverState";
import { useEvent } from 'rc-util';
var getTitleFromCellRenderChildren = function getTitleFromCellRenderChildren(_ref) {
  var ellipsis = _ref.ellipsis,
    rowType = _ref.rowType,
    children = _ref.children;
  var title;
  var ellipsisConfig = ellipsis === true ? {
    showTitle: true
  } : ellipsis;
  if (ellipsisConfig && (ellipsisConfig.showTitle || rowType === 'header')) {
    if (typeof children === 'string' || typeof children === 'number') {
      title = children.toString();
    } else if ( /*#__PURE__*/React.isValidElement(children) && typeof children.props.children === 'string') {
      title = children.props.children;
    }
  }
  return title;
};
function Cell(props) {
  var _ref2, _ref3, _legacyCellProps$colS, _ref4, _ref5, _legacyCellProps$rowS, _additionalProps$titl, _classNames;
  if (process.env.NODE_ENV !== 'production') {
    devRenderTimes(props);
  }
  var Component = props.component,
    children = props.children,
    ellipsis = props.ellipsis,
    scope = props.scope,
    prefixCls = props.prefixCls,
    className = props.className,
    align = props.align,
    record = props.record,
    render = props.render,
    dataIndex = props.dataIndex,
    renderIndex = props.renderIndex,
    shouldCellUpdate = props.shouldCellUpdate,
    index = props.index,
    rowType = props.rowType,
    colSpan = props.colSpan,
    rowSpan = props.rowSpan,
    fixLeft = props.fixLeft,
    fixRight = props.fixRight,
    firstFixLeft = props.firstFixLeft,
    lastFixLeft = props.lastFixLeft,
    firstFixRight = props.firstFixRight,
    lastFixRight = props.lastFixRight,
    appendNode = props.appendNode,
    _props$additionalProp = props.additionalProps,
    additionalProps = _props$additionalProp === void 0 ? {} : _props$additionalProp,
    isSticky = props.isSticky;
  var cellPrefixCls = "".concat(prefixCls, "-cell");
  var _useContext = useContext(TableContext, ['supportSticky', 'allColumnsFixedLeft']),
    supportSticky = _useContext.supportSticky,
    allColumnsFixedLeft = _useContext.allColumnsFixedLeft;

  // ====================== Value =======================
  var _useCellRender = useCellRender(record, dataIndex, renderIndex, children, render, shouldCellUpdate),
    _useCellRender2 = _slicedToArray(_useCellRender, 2),
    childNode = _useCellRender2[0],
    legacyCellProps = _useCellRender2[1];

  // ====================== Fixed =======================
  var fixedStyle = {};
  var isFixLeft = typeof fixLeft === 'number' && supportSticky;
  var isFixRight = typeof fixRight === 'number' && supportSticky;
  if (isFixLeft) {
    fixedStyle.position = 'sticky';
    fixedStyle.left = fixLeft;
  }
  if (isFixRight) {
    fixedStyle.position = 'sticky';
    fixedStyle.right = fixRight;
  }

  // ================ RowSpan & ColSpan =================
  var mergedColSpan = (_ref2 = (_ref3 = (_legacyCellProps$colS = legacyCellProps === null || legacyCellProps === void 0 ? void 0 : legacyCellProps.colSpan) !== null && _legacyCellProps$colS !== void 0 ? _legacyCellProps$colS : additionalProps.colSpan) !== null && _ref3 !== void 0 ? _ref3 : colSpan) !== null && _ref2 !== void 0 ? _ref2 : 1;
  var mergedRowSpan = (_ref4 = (_ref5 = (_legacyCellProps$rowS = legacyCellProps === null || legacyCellProps === void 0 ? void 0 : legacyCellProps.rowSpan) !== null && _legacyCellProps$rowS !== void 0 ? _legacyCellProps$rowS : additionalProps.rowSpan) !== null && _ref5 !== void 0 ? _ref5 : rowSpan) !== null && _ref4 !== void 0 ? _ref4 : 1;

  // ====================== Hover =======================
  var _useHoverState = useHoverState(index, mergedRowSpan),
    _useHoverState2 = _slicedToArray(_useHoverState, 2),
    hovering = _useHoverState2[0],
    onHover = _useHoverState2[1];
  var onMouseEnter = useEvent(function (event) {
    var _additionalProps$onMo;
    if (record) {
      onHover(index, index + mergedRowSpan - 1);
    }
    additionalProps === null || additionalProps === void 0 || (_additionalProps$onMo = additionalProps.onMouseEnter) === null || _additionalProps$onMo === void 0 || _additionalProps$onMo.call(additionalProps, event);
  });
  var onMouseLeave = useEvent(function (event) {
    var _additionalProps$onMo2;
    if (record) {
      onHover(-1, -1);
    }
    additionalProps === null || additionalProps === void 0 || (_additionalProps$onMo2 = additionalProps.onMouseLeave) === null || _additionalProps$onMo2 === void 0 || _additionalProps$onMo2.call(additionalProps, event);
  });

  // ====================== Render ======================
  if (mergedColSpan === 0 || mergedRowSpan === 0) {
    return null;
  }

  // >>>>> Title
  var title = (_additionalProps$titl = additionalProps.title) !== null && _additionalProps$titl !== void 0 ? _additionalProps$titl : getTitleFromCellRenderChildren({
    rowType: rowType,
    ellipsis: ellipsis,
    children: childNode
  });

  // >>>>> ClassName
  var mergedClassName = classNames(cellPrefixCls, className, (_classNames = {}, _defineProperty(_classNames, "".concat(cellPrefixCls, "-fix-left"), isFixLeft && supportSticky), _defineProperty(_classNames, "".concat(cellPrefixCls, "-fix-left-first"), firstFixLeft && supportSticky), _defineProperty(_classNames, "".concat(cellPrefixCls, "-fix-left-last"), lastFixLeft && supportSticky), _defineProperty(_classNames, "".concat(cellPrefixCls, "-fix-left-all"), lastFixLeft && allColumnsFixedLeft && supportSticky), _defineProperty(_classNames, "".concat(cellPrefixCls, "-fix-right"), isFixRight && supportSticky), _defineProperty(_classNames, "".concat(cellPrefixCls, "-fix-right-first"), firstFixRight && supportSticky), _defineProperty(_classNames, "".concat(cellPrefixCls, "-fix-right-last"), lastFixRight && supportSticky), _defineProperty(_classNames, "".concat(cellPrefixCls, "-ellipsis"), ellipsis), _defineProperty(_classNames, "".concat(cellPrefixCls, "-with-append"), appendNode), _defineProperty(_classNames, "".concat(cellPrefixCls, "-fix-sticky"), (isFixLeft || isFixRight) && isSticky && supportSticky), _defineProperty(_classNames, "".concat(cellPrefixCls, "-row-hover"), !legacyCellProps && hovering), _classNames), additionalProps.className, legacyCellProps === null || legacyCellProps === void 0 ? void 0 : legacyCellProps.className);

  // >>>>> Style
  var alignStyle = {};
  if (align) {
    alignStyle.textAlign = align;
  }
  var mergedStyle = _objectSpread(_objectSpread(_objectSpread(_objectSpread({}, fixedStyle), additionalProps.style), alignStyle), legacyCellProps === null || legacyCellProps === void 0 ? void 0 : legacyCellProps.style);

  // >>>>> Children Node
  var mergedChildNode = childNode;

  // Not crash if final `childNode` is not validate ReactNode
  if (_typeof(mergedChildNode) === 'object' && !Array.isArray(mergedChildNode) && ! /*#__PURE__*/React.isValidElement(mergedChildNode)) {
    mergedChildNode = null;
  }
  if (ellipsis && (lastFixLeft || firstFixRight)) {
    mergedChildNode = /*#__PURE__*/React.createElement("span", {
      className: "".concat(cellPrefixCls, "-content")
    }, mergedChildNode);
  }
  return /*#__PURE__*/React.createElement(Component, _extends({}, legacyCellProps, additionalProps, {
    className: mergedClassName,
    style: mergedStyle
    // A11y
    ,
    title: title,
    scope: scope
    // Hover
    ,
    onMouseEnter: onMouseEnter,
    onMouseLeave: onMouseLeave
    //Span
    ,
    colSpan: mergedColSpan !== 1 ? mergedColSpan : null,
    rowSpan: mergedRowSpan !== 1 ? mergedRowSpan : null
  }), appendNode, mergedChildNode);
}
export default /*#__PURE__*/React.memo(Cell);