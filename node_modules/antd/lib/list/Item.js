"use strict";
"use client";

var _interopRequireWildcard = require("@babel/runtime/helpers/interopRequireWildcard").default;
var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault").default;
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = exports.Meta = void 0;
var _classnames = _interopRequireDefault(require("classnames"));
var _react = _interopRequireWildcard(require("react"));
var _reactNode = require("../_util/reactNode");
var _configProvider = require("../config-provider");
var _grid = require("../grid");
var _context = require("./context");
var __rest = void 0 && (void 0).__rest || function (s, e) {
  var t = {};
  for (var p in s) if (Object.prototype.hasOwnProperty.call(s, p) && e.indexOf(p) < 0) t[p] = s[p];
  if (s != null && typeof Object.getOwnPropertySymbols === "function") for (var i = 0, p = Object.getOwnPropertySymbols(s); i < p.length; i++) {
    if (e.indexOf(p[i]) < 0 && Object.prototype.propertyIsEnumerable.call(s, p[i])) t[p[i]] = s[p[i]];
  }
  return t;
};
const Meta = _a => {
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
  } = (0, _react.useContext)(_configProvider.ConfigContext);
  const prefixCls = getPrefixCls('list', customizePrefixCls);
  const classString = (0, _classnames.default)(`${prefixCls}-item-meta`, className);
  const content = /*#__PURE__*/_react.default.createElement("div", {
    className: `${prefixCls}-item-meta-content`
  }, title && /*#__PURE__*/_react.default.createElement("h4", {
    className: `${prefixCls}-item-meta-title`
  }, title), description && /*#__PURE__*/_react.default.createElement("div", {
    className: `${prefixCls}-item-meta-description`
  }, description));
  return /*#__PURE__*/_react.default.createElement("div", Object.assign({}, others, {
    className: classString
  }), avatar && /*#__PURE__*/_react.default.createElement("div", {
    className: `${prefixCls}-item-meta-avatar`
  }, avatar), (title || description) && content);
};
exports.Meta = Meta;
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
  } = (0, _react.useContext)(_context.ListContext);
  const {
    getPrefixCls
  } = (0, _react.useContext)(_configProvider.ConfigContext);
  const isItemContainsTextNodeAndNotSingular = () => {
    let result;
    _react.Children.forEach(children, element => {
      if (typeof element === 'string') {
        result = true;
      }
    });
    return result && _react.Children.count(children) > 1;
  };
  const isFlexMode = () => {
    if (itemLayout === 'vertical') {
      return !!extra;
    }
    return !isItemContainsTextNodeAndNotSingular();
  };
  const prefixCls = getPrefixCls('list', customizePrefixCls);
  const actionsContent = actions && actions.length > 0 && ( /*#__PURE__*/_react.default.createElement("ul", {
    className: `${prefixCls}-item-action`,
    key: "actions"
  }, actions.map((action, i) => (
  /*#__PURE__*/
  // eslint-disable-next-line react/no-array-index-key
  _react.default.createElement("li", {
    key: `${prefixCls}-item-action-${i}`
  }, action, i !== actions.length - 1 && /*#__PURE__*/_react.default.createElement("em", {
    className: `${prefixCls}-item-action-split`
  }))))));
  const Element = grid ? 'div' : 'li';
  const itemChildren = /*#__PURE__*/_react.default.createElement(Element, Object.assign({}, others, !grid ? {
    ref
  } : {}, {
    className: (0, _classnames.default)(`${prefixCls}-item`, {
      [`${prefixCls}-item-no-flex`]: !isFlexMode()
    }, className)
  }), itemLayout === 'vertical' && extra ? [/*#__PURE__*/_react.default.createElement("div", {
    className: `${prefixCls}-item-main`,
    key: "content"
  }, children, actionsContent), /*#__PURE__*/_react.default.createElement("div", {
    className: `${prefixCls}-item-extra`,
    key: "extra"
  }, extra)] : [children, actionsContent, (0, _reactNode.cloneElement)(extra, {
    key: 'extra'
  })]);
  return grid ? ( /*#__PURE__*/_react.default.createElement(_grid.Col, {
    ref: ref,
    flex: 1,
    style: colStyle
  }, itemChildren)) : itemChildren;
};
const Item = /*#__PURE__*/(0, _react.forwardRef)(InternalItem);
Item.Meta = Meta;
var _default = exports.default = Item;