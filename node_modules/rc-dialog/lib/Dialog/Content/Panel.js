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
var _classnames = _interopRequireDefault(require("classnames"));
var _ref = require("rc-util/lib/ref");
var _react = _interopRequireWildcard(require("react"));
var _context = require("../../context");
var _MemoChildren = _interopRequireDefault(require("./MemoChildren"));
var _pickAttrs = _interopRequireDefault(require("rc-util/lib/pickAttrs"));
function _getRequireWildcardCache(e) { if ("function" != typeof WeakMap) return null; var r = new WeakMap(), t = new WeakMap(); return (_getRequireWildcardCache = function _getRequireWildcardCache(e) { return e ? t : r; })(e); }
function _interopRequireWildcard(e, r) { if (!r && e && e.__esModule) return e; if (null === e || "object" != _typeof3(e) && "function" != typeof e) return { default: e }; var t = _getRequireWildcardCache(r); if (t && t.has(e)) return t.get(e); var n = { __proto__: null }, a = Object.defineProperty && Object.getOwnPropertyDescriptor; for (var u in e) if ("default" !== u && Object.prototype.hasOwnProperty.call(e, u)) { var i = a ? Object.getOwnPropertyDescriptor(e, u) : null; i && (i.get || i.set) ? Object.defineProperty(n, u, i) : n[u] = e[u]; } return n.default = e, t && t.set(e, n), n; }
var sentinelStyle = {
  width: 0,
  height: 0,
  overflow: 'hidden',
  outline: 'none'
};
var entityStyle = {
  outline: 'none'
};
var Panel = /*#__PURE__*/_react.default.forwardRef(function (props, ref) {
  var prefixCls = props.prefixCls,
    className = props.className,
    style = props.style,
    title = props.title,
    ariaId = props.ariaId,
    footer = props.footer,
    closable = props.closable,
    closeIcon = props.closeIcon,
    onClose = props.onClose,
    children = props.children,
    bodyStyle = props.bodyStyle,
    bodyProps = props.bodyProps,
    modalRender = props.modalRender,
    onMouseDown = props.onMouseDown,
    onMouseUp = props.onMouseUp,
    holderRef = props.holderRef,
    visible = props.visible,
    forceRender = props.forceRender,
    width = props.width,
    height = props.height,
    modalClassNames = props.classNames,
    modalStyles = props.styles;

  // ================================= Refs =================================
  var _React$useContext = _react.default.useContext(_context.RefContext),
    panelRef = _React$useContext.panel;
  var mergedRef = (0, _ref.useComposeRef)(holderRef, panelRef);
  var sentinelStartRef = (0, _react.useRef)();
  var sentinelEndRef = (0, _react.useRef)();
  var entityRef = (0, _react.useRef)();
  _react.default.useImperativeHandle(ref, function () {
    return {
      focus: function focus() {
        var _entityRef$current;
        (_entityRef$current = entityRef.current) === null || _entityRef$current === void 0 || _entityRef$current.focus();
      },
      changeActive: function changeActive(next) {
        var _document = document,
          activeElement = _document.activeElement;
        if (next && activeElement === sentinelEndRef.current) {
          sentinelStartRef.current.focus();
        } else if (!next && activeElement === sentinelStartRef.current) {
          sentinelEndRef.current.focus();
        }
      }
    };
  });

  // ================================ Style =================================
  var contentStyle = {};
  if (width !== undefined) {
    contentStyle.width = width;
  }
  if (height !== undefined) {
    contentStyle.height = height;
  }
  // ================================ Render ================================
  var footerNode;
  if (footer) {
    footerNode = /*#__PURE__*/_react.default.createElement("div", {
      className: (0, _classnames.default)("".concat(prefixCls, "-footer"), modalClassNames === null || modalClassNames === void 0 ? void 0 : modalClassNames.footer),
      style: (0, _objectSpread2.default)({}, modalStyles === null || modalStyles === void 0 ? void 0 : modalStyles.footer)
    }, footer);
  }
  var headerNode;
  if (title) {
    headerNode = /*#__PURE__*/_react.default.createElement("div", {
      className: (0, _classnames.default)("".concat(prefixCls, "-header"), modalClassNames === null || modalClassNames === void 0 ? void 0 : modalClassNames.header),
      style: (0, _objectSpread2.default)({}, modalStyles === null || modalStyles === void 0 ? void 0 : modalStyles.header)
    }, /*#__PURE__*/_react.default.createElement("div", {
      className: "".concat(prefixCls, "-title"),
      id: ariaId
    }, title));
  }
  var closableObj = (0, _react.useMemo)(function () {
    if ((0, _typeof2.default)(closable) === 'object' && closable !== null) {
      return closable;
    }
    if (closable) {
      return {
        closeIcon: closeIcon !== null && closeIcon !== void 0 ? closeIcon : /*#__PURE__*/_react.default.createElement("span", {
          className: "".concat(prefixCls, "-close-x")
        })
      };
    }
    return {};
  }, [closable, closeIcon]);
  var ariaProps = (0, _pickAttrs.default)(closableObj, true);
  var closer;
  if (closable) {
    closer = /*#__PURE__*/_react.default.createElement("button", (0, _extends2.default)({
      type: "button",
      onClick: onClose,
      "aria-label": "Close"
    }, ariaProps, {
      className: "".concat(prefixCls, "-close")
    }), closableObj.closeIcon);
  }
  var content = /*#__PURE__*/_react.default.createElement("div", {
    className: (0, _classnames.default)("".concat(prefixCls, "-content"), modalClassNames === null || modalClassNames === void 0 ? void 0 : modalClassNames.content),
    style: modalStyles === null || modalStyles === void 0 ? void 0 : modalStyles.content
  }, closer, headerNode, /*#__PURE__*/_react.default.createElement("div", (0, _extends2.default)({
    className: (0, _classnames.default)("".concat(prefixCls, "-body"), modalClassNames === null || modalClassNames === void 0 ? void 0 : modalClassNames.body),
    style: (0, _objectSpread2.default)((0, _objectSpread2.default)({}, bodyStyle), modalStyles === null || modalStyles === void 0 ? void 0 : modalStyles.body)
  }, bodyProps), children), footerNode);
  return /*#__PURE__*/_react.default.createElement("div", {
    key: "dialog-element",
    role: "dialog",
    "aria-labelledby": title ? ariaId : null,
    "aria-modal": "true",
    ref: mergedRef,
    style: (0, _objectSpread2.default)((0, _objectSpread2.default)({}, style), contentStyle),
    className: (0, _classnames.default)(prefixCls, className),
    onMouseDown: onMouseDown,
    onMouseUp: onMouseUp
  }, /*#__PURE__*/_react.default.createElement("div", {
    tabIndex: 0,
    ref: sentinelStartRef,
    style: sentinelStyle,
    "aria-hidden": "true"
  }), /*#__PURE__*/_react.default.createElement("div", {
    ref: entityRef,
    tabIndex: -1,
    style: entityStyle
  }, /*#__PURE__*/_react.default.createElement(_MemoChildren.default, {
    shouldUpdate: visible || forceRender
  }, modalRender ? modalRender(content) : content)), /*#__PURE__*/_react.default.createElement("div", {
    tabIndex: 0,
    ref: sentinelEndRef,
    style: sentinelStyle,
    "aria-hidden": "true"
  }));
});
if (process.env.NODE_ENV !== 'production') {
  Panel.displayName = 'Panel';
}
var _default = exports.default = Panel;