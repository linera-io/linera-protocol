"use strict";
"use client";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault").default;
var _interopRequireWildcard = require("@babel/runtime/helpers/interopRequireWildcard").default;
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = void 0;
var React = _interopRequireWildcard(require("react"));
var _classnames = _interopRequireDefault(require("classnames"));
var _rcMenu = require("rc-menu");
var _omit = _interopRequireDefault(require("rc-util/lib/omit"));
var _useZIndex = require("../_util/hooks/useZIndex");
var _reactNode = require("../_util/reactNode");
var _MenuContext = _interopRequireDefault(require("./MenuContext"));
const SubMenu = props => {
  var _a;
  const {
    popupClassName,
    icon,
    title,
    theme: customTheme
  } = props;
  const context = React.useContext(_MenuContext.default);
  const {
    prefixCls,
    inlineCollapsed,
    theme: contextTheme
  } = context;
  const parentPath = (0, _rcMenu.useFullPath)();
  let titleNode;
  if (!icon) {
    titleNode = inlineCollapsed && !parentPath.length && title && typeof title === 'string' ? ( /*#__PURE__*/React.createElement("div", {
      className: `${prefixCls}-inline-collapsed-noicon`
    }, title.charAt(0))) : ( /*#__PURE__*/React.createElement("span", {
      className: `${prefixCls}-title-content`
    }, title));
  } else {
    // inline-collapsed.md demo 依赖 span 来隐藏文字,有 icon 属性，则内部包裹一个 span
    // ref: https://github.com/ant-design/ant-design/pull/23456
    const titleIsSpan = /*#__PURE__*/React.isValidElement(title) && title.type === 'span';
    titleNode = /*#__PURE__*/React.createElement(React.Fragment, null, (0, _reactNode.cloneElement)(icon, {
      className: (0, _classnames.default)( /*#__PURE__*/React.isValidElement(icon) ? (_a = icon.props) === null || _a === void 0 ? void 0 : _a.className : '', `${prefixCls}-item-icon`)
    }), titleIsSpan ? title : /*#__PURE__*/React.createElement("span", {
      className: `${prefixCls}-title-content`
    }, title));
  }
  const contextValue = React.useMemo(() => Object.assign(Object.assign({}, context), {
    firstLevel: false
  }), [context]);
  // ============================ zIndex ============================
  const [zIndex] = (0, _useZIndex.useZIndex)('Menu');
  return /*#__PURE__*/React.createElement(_MenuContext.default.Provider, {
    value: contextValue
  }, /*#__PURE__*/React.createElement(_rcMenu.SubMenu, Object.assign({}, (0, _omit.default)(props, ['icon']), {
    title: titleNode,
    popupClassName: (0, _classnames.default)(prefixCls, popupClassName, `${prefixCls}-${customTheme || contextTheme}`),
    popupStyle: {
      zIndex
    }
  })));
};
var _default = exports.default = SubMenu;