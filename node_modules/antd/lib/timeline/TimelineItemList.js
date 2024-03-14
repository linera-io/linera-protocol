"use strict";
"use client";

var _interopRequireWildcard = require("@babel/runtime/helpers/interopRequireWildcard").default;
var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault").default;
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = void 0;
var _toConsumableArray2 = _interopRequireDefault(require("@babel/runtime/helpers/toConsumableArray"));
var _classnames = _interopRequireDefault(require("classnames"));
var React = _interopRequireWildcard(require("react"));
var _LoadingOutlined = _interopRequireDefault(require("@ant-design/icons/LoadingOutlined"));
var _TimelineItem = _interopRequireDefault(require("./TimelineItem"));
var __rest = void 0 && (void 0).__rest || function (s, e) {
  var t = {};
  for (var p in s) if (Object.prototype.hasOwnProperty.call(s, p) && e.indexOf(p) < 0) t[p] = s[p];
  if (s != null && typeof Object.getOwnPropertySymbols === "function") for (var i = 0, p = Object.getOwnPropertySymbols(s); i < p.length; i++) {
    if (e.indexOf(p[i]) < 0 && Object.prototype.propertyIsEnumerable.call(s, p[i])) t[p[i]] = s[p[i]];
  }
  return t;
};
const TimelineItemList = _a => {
  var {
      prefixCls,
      className,
      pending = false,
      children,
      items,
      rootClassName,
      reverse = false,
      direction,
      hashId,
      pendingDot,
      mode = ''
    } = _a,
    restProps = __rest(_a, ["prefixCls", "className", "pending", "children", "items", "rootClassName", "reverse", "direction", "hashId", "pendingDot", "mode"]);
  const getPositionCls = (position, idx) => {
    if (mode === 'alternate') {
      if (position === 'right') return `${prefixCls}-item-right`;
      if (position === 'left') return `${prefixCls}-item-left`;
      return idx % 2 === 0 ? `${prefixCls}-item-left` : `${prefixCls}-item-right`;
    }
    if (mode === 'left') return `${prefixCls}-item-left`;
    if (mode === 'right') return `${prefixCls}-item-right`;
    if (position === 'right') return `${prefixCls}-item-right`;
    return '';
  };
  const mergedItems = (0, _toConsumableArray2.default)(items || []);
  const pendingNode = typeof pending === 'boolean' ? null : pending;
  if (pending) {
    mergedItems.push({
      pending: !!pending,
      dot: pendingDot || /*#__PURE__*/React.createElement(_LoadingOutlined.default, null),
      children: pendingNode
    });
  }
  if (reverse) {
    mergedItems.reverse();
  }
  const itemsCount = mergedItems.length;
  const lastCls = `${prefixCls}-item-last`;
  const itemsList = mergedItems.filter(item => !!item).map((item, idx) => {
    var _a;
    const pendingClass = idx === itemsCount - 2 ? lastCls : '';
    const readyClass = idx === itemsCount - 1 ? lastCls : '';
    const {
        className: itemClassName
      } = item,
      itemProps = __rest(item, ["className"]);
    return /*#__PURE__*/React.createElement(_TimelineItem.default, Object.assign({}, itemProps, {
      className: (0, _classnames.default)([itemClassName, !reverse && !!pending ? pendingClass : readyClass, getPositionCls((_a = item === null || item === void 0 ? void 0 : item.position) !== null && _a !== void 0 ? _a : '', idx)]),
      /* eslint-disable-next-line react/no-array-index-key */
      key: (item === null || item === void 0 ? void 0 : item.key) || idx
    }));
  });
  const hasLabelItem = mergedItems.some(item => !!(item === null || item === void 0 ? void 0 : item.label));
  const classString = (0, _classnames.default)(prefixCls, {
    [`${prefixCls}-pending`]: !!pending,
    [`${prefixCls}-reverse`]: !!reverse,
    [`${prefixCls}-${mode}`]: !!mode && !hasLabelItem,
    [`${prefixCls}-label`]: hasLabelItem,
    [`${prefixCls}-rtl`]: direction === 'rtl'
  }, className, rootClassName, hashId);
  return /*#__PURE__*/React.createElement("ul", Object.assign({}, restProps, {
    className: classString
  }), itemsList);
};
var _default = exports.default = TimelineItemList;