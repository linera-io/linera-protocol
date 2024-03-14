"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault");
var _typeof3 = require("@babel/runtime/helpers/typeof");
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = void 0;
var _extends2 = _interopRequireDefault(require("@babel/runtime/helpers/extends"));
var _typeof2 = _interopRequireDefault(require("@babel/runtime/helpers/typeof"));
var _objectSpread2 = _interopRequireDefault(require("@babel/runtime/helpers/objectSpread2"));
var _defineProperty2 = _interopRequireDefault(require("@babel/runtime/helpers/defineProperty"));
var _slicedToArray2 = _interopRequireDefault(require("@babel/runtime/helpers/slicedToArray"));
var _context = require("@rc-component/context");
var _classnames = _interopRequireDefault(require("classnames"));
var React = _interopRequireWildcard(require("react"));
var _TableContext = _interopRequireDefault(require("../context/TableContext"));
var _useRenderTimes = _interopRequireDefault(require("../hooks/useRenderTimes"));
var _useCellRender3 = _interopRequireDefault(require("./useCellRender"));
var _useHoverState3 = _interopRequireDefault(require("./useHoverState"));
var _rcUtil = require("rc-util");
function _getRequireWildcardCache(e) { if ("function" != typeof WeakMap) return null; var r = new WeakMap(), t = new WeakMap(); return (_getRequireWildcardCache = function _getRequireWildcardCache(e) { return e ? t : r; })(e); }
function _interopRequireWildcard(e, r) { if (!r && e && e.__esModule) return e; if (null === e || "object" != _typeof3(e) && "function" != typeof e) return { default: e }; var t = _getRequireWildcardCache(r); if (t && t.has(e)) return t.get(e); var n = { __proto__: null }, a = Object.defineProperty && Object.getOwnPropertyDescriptor; for (var u in e) if ("default" !== u && Object.prototype.hasOwnProperty.call(e, u)) { var i = a ? Object.getOwnPropertyDescriptor(e, u) : null; i && (i.get || i.set) ? Object.defineProperty(n, u, i) : n[u] = e[u]; } return n.default = e, t && t.set(e, n), n; }
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
    (0, _useRenderTimes.default)(props);
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
  var _useContext = (0, _context.useContext)(_TableContext.default, ['supportSticky', 'allColumnsFixedLeft']),
    supportSticky = _useContext.supportSticky,
    allColumnsFixedLeft = _useContext.allColumnsFixedLeft;

  // ====================== Value =======================
  var _useCellRender = (0, _useCellRender3.default)(record, dataIndex, renderIndex, children, render, shouldCellUpdate),
    _useCellRender2 = (0, _slicedToArray2.default)(_useCellRender, 2),
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
  var _useHoverState = (0, _useHoverState3.default)(index, mergedRowSpan),
    _useHoverState2 = (0, _slicedToArray2.default)(_useHoverState, 2),
    hovering = _useHoverState2[0],
    onHover = _useHoverState2[1];
  var onMouseEnter = (0, _rcUtil.useEvent)(function (event) {
    var _additionalProps$onMo;
    if (record) {
      onHover(index, index + mergedRowSpan - 1);
    }
    additionalProps === null || additionalProps === void 0 || (_additionalProps$onMo = additionalProps.onMouseEnter) === null || _additionalProps$onMo === void 0 || _additionalProps$onMo.call(additionalProps, event);
  });
  var onMouseLeave = (0, _rcUtil.useEvent)(function (event) {
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
  var mergedClassName = (0, _classnames.default)(cellPrefixCls, className, (_classNames = {}, (0, _defineProperty2.default)(_classNames, "".concat(cellPrefixCls, "-fix-left"), isFixLeft && supportSticky), (0, _defineProperty2.default)(_classNames, "".concat(cellPrefixCls, "-fix-left-first"), firstFixLeft && supportSticky), (0, _defineProperty2.default)(_classNames, "".concat(cellPrefixCls, "-fix-left-last"), lastFixLeft && supportSticky), (0, _defineProperty2.default)(_classNames, "".concat(cellPrefixCls, "-fix-left-all"), lastFixLeft && allColumnsFixedLeft && supportSticky), (0, _defineProperty2.default)(_classNames, "".concat(cellPrefixCls, "-fix-right"), isFixRight && supportSticky), (0, _defineProperty2.default)(_classNames, "".concat(cellPrefixCls, "-fix-right-first"), firstFixRight && supportSticky), (0, _defineProperty2.default)(_classNames, "".concat(cellPrefixCls, "-fix-right-last"), lastFixRight && supportSticky), (0, _defineProperty2.default)(_classNames, "".concat(cellPrefixCls, "-ellipsis"), ellipsis), (0, _defineProperty2.default)(_classNames, "".concat(cellPrefixCls, "-with-append"), appendNode), (0, _defineProperty2.default)(_classNames, "".concat(cellPrefixCls, "-fix-sticky"), (isFixLeft || isFixRight) && isSticky && supportSticky), (0, _defineProperty2.default)(_classNames, "".concat(cellPrefixCls, "-row-hover"), !legacyCellProps && hovering), _classNames), additionalProps.className, legacyCellProps === null || legacyCellProps === void 0 ? void 0 : legacyCellProps.className);

  // >>>>> Style
  var alignStyle = {};
  if (align) {
    alignStyle.textAlign = align;
  }
  var mergedStyle = (0, _objectSpread2.default)((0, _objectSpread2.default)((0, _objectSpread2.default)((0, _objectSpread2.default)({}, fixedStyle), additionalProps.style), alignStyle), legacyCellProps === null || legacyCellProps === void 0 ? void 0 : legacyCellProps.style);

  // >>>>> Children Node
  var mergedChildNode = childNode;

  // Not crash if final `childNode` is not validate ReactNode
  if ((0, _typeof2.default)(mergedChildNode) === 'object' && !Array.isArray(mergedChildNode) && ! /*#__PURE__*/React.isValidElement(mergedChildNode)) {
    mergedChildNode = null;
  }
  if (ellipsis && (lastFixLeft || firstFixRight)) {
    mergedChildNode = /*#__PURE__*/React.createElement("span", {
      className: "".concat(cellPrefixCls, "-content")
    }, mergedChildNode);
  }
  return /*#__PURE__*/React.createElement(Component, (0, _extends2.default)({}, legacyCellProps, additionalProps, {
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
var _default = exports.default = /*#__PURE__*/React.memo(Cell);