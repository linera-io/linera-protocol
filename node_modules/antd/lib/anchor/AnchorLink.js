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
var _warning = require("../_util/warning");
var _configProvider = require("../config-provider");
var _context = _interopRequireDefault(require("./context"));
const AnchorLink = props => {
  const {
    href,
    title,
    prefixCls: customizePrefixCls,
    children,
    className,
    target,
    replace
  } = props;
  const context = React.useContext(_context.default);
  const {
    registerLink,
    unregisterLink,
    scrollTo,
    onClick,
    activeLink,
    direction
  } = context || {};
  React.useEffect(() => {
    registerLink === null || registerLink === void 0 ? void 0 : registerLink(href);
    return () => {
      unregisterLink === null || unregisterLink === void 0 ? void 0 : unregisterLink(href);
    };
  }, [href]);
  const handleClick = e => {
    if (replace) {
      e.preventDefault();
      window.location.replace(href);
    }
    onClick === null || onClick === void 0 ? void 0 : onClick(e, {
      title,
      href
    });
    scrollTo === null || scrollTo === void 0 ? void 0 : scrollTo(href);
  };
  // =================== Warning =====================
  if (process.env.NODE_ENV !== 'production') {
    const warning = (0, _warning.devUseWarning)('Anchor.Link');
    process.env.NODE_ENV !== "production" ? warning(!children || direction !== 'horizontal', 'usage', '`Anchor.Link children` is not supported when `Anchor` direction is horizontal') : void 0;
  }
  const {
    getPrefixCls
  } = React.useContext(_configProvider.ConfigContext);
  const prefixCls = getPrefixCls('anchor', customizePrefixCls);
  const active = activeLink === href;
  const wrapperClassName = (0, _classnames.default)(`${prefixCls}-link`, className, {
    [`${prefixCls}-link-active`]: active
  });
  const titleClassName = (0, _classnames.default)(`${prefixCls}-link-title`, {
    [`${prefixCls}-link-title-active`]: active
  });
  return /*#__PURE__*/React.createElement("div", {
    className: wrapperClassName
  }, /*#__PURE__*/React.createElement("a", {
    className: titleClassName,
    href: href,
    title: typeof title === 'string' ? title : '',
    target: target,
    onClick: handleClick
  }, title), direction !== 'horizontal' ? children : null);
};
var _default = exports.default = AnchorLink;