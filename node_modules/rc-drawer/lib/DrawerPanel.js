"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault").default;
var _interopRequireWildcard = require("@babel/runtime/helpers/interopRequireWildcard").default;
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = void 0;
var _extends2 = _interopRequireDefault(require("@babel/runtime/helpers/extends"));
var _objectWithoutProperties2 = _interopRequireDefault(require("@babel/runtime/helpers/objectWithoutProperties"));
var _classnames = _interopRequireDefault(require("classnames"));
var React = _interopRequireWildcard(require("react"));
var _context = require("./context");
var _pickAttrs = _interopRequireDefault(require("rc-util/lib/pickAttrs"));
var _ref = require("rc-util/lib/ref");
var _excluded = ["prefixCls", "className", "containerRef"];
var DrawerPanel = function DrawerPanel(props) {
  var prefixCls = props.prefixCls,
    className = props.className,
    containerRef = props.containerRef,
    restProps = (0, _objectWithoutProperties2.default)(props, _excluded);
  var _React$useContext = React.useContext(_context.RefContext),
    panelRef = _React$useContext.panel;
  var mergedRef = (0, _ref.useComposeRef)(panelRef, containerRef);

  // =============================== Render ===============================

  return /*#__PURE__*/React.createElement("div", (0, _extends2.default)({
    className: (0, _classnames.default)("".concat(prefixCls, "-content"), className),
    role: "dialog",
    ref: mergedRef
  }, (0, _pickAttrs.default)(props, {
    aria: true
  }), {
    "aria-modal": "true"
  }, restProps));
};
if (process.env.NODE_ENV !== 'production') {
  DrawerPanel.displayName = 'DrawerPanel';
}
var _default = exports.default = DrawerPanel;