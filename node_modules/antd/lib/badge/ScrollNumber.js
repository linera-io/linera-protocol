"use strict";
"use client";

var _interopRequireWildcard = require("@babel/runtime/helpers/interopRequireWildcard").default;
var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault").default;
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = void 0;
var _classnames = _interopRequireDefault(require("classnames"));
var React = _interopRequireWildcard(require("react"));
var _reactNode = require("../_util/reactNode");
var _configProvider = require("../config-provider");
var _SingleNumber = _interopRequireDefault(require("./SingleNumber"));
var __rest = void 0 && (void 0).__rest || function (s, e) {
  var t = {};
  for (var p in s) if (Object.prototype.hasOwnProperty.call(s, p) && e.indexOf(p) < 0) t[p] = s[p];
  if (s != null && typeof Object.getOwnPropertySymbols === "function") for (var i = 0, p = Object.getOwnPropertySymbols(s); i < p.length; i++) {
    if (e.indexOf(p[i]) < 0 && Object.prototype.propertyIsEnumerable.call(s, p[i])) t[p[i]] = s[p[i]];
  }
  return t;
};
const ScrollNumber = /*#__PURE__*/React.forwardRef((props, ref) => {
  const {
      prefixCls: customizePrefixCls,
      count,
      className,
      motionClassName,
      style,
      title,
      show,
      component: Component = 'sup',
      children
    } = props,
    restProps = __rest(props, ["prefixCls", "count", "className", "motionClassName", "style", "title", "show", "component", "children"]);
  const {
    getPrefixCls
  } = React.useContext(_configProvider.ConfigContext);
  const prefixCls = getPrefixCls('scroll-number', customizePrefixCls);
  // ============================ Render ============================
  const newProps = Object.assign(Object.assign({}, restProps), {
    'data-show': show,
    style,
    className: (0, _classnames.default)(prefixCls, className, motionClassName),
    title: title
  });
  // Only integer need motion
  let numberNodes = count;
  if (count && Number(count) % 1 === 0) {
    const numberList = String(count).split('');
    numberNodes = /*#__PURE__*/React.createElement("bdi", null, numberList.map((num, i) => ( /*#__PURE__*/React.createElement(_SingleNumber.default, {
      prefixCls: prefixCls,
      count: Number(count),
      value: num,
      // eslint-disable-next-line react/no-array-index-key
      key: numberList.length - i
    }))));
  }
  // allow specify the border
  // mock border-color by box-shadow for compatible with old usage:
  // <Badge count={4} style={{ backgroundColor: '#fff', color: '#999', borderColor: '#d9d9d9' }} />
  if (style && style.borderColor) {
    newProps.style = Object.assign(Object.assign({}, style), {
      boxShadow: `0 0 0 1px ${style.borderColor} inset`
    });
  }
  if (children) {
    return (0, _reactNode.cloneElement)(children, oriProps => ({
      className: (0, _classnames.default)(`${prefixCls}-custom-component`, oriProps === null || oriProps === void 0 ? void 0 : oriProps.className, motionClassName)
    }));
  }
  return /*#__PURE__*/React.createElement(Component, Object.assign({}, newProps, {
    ref: ref
  }), numberNodes);
});
var _default = exports.default = ScrollNumber;