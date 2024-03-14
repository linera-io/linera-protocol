"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault");
var _typeof = require("@babel/runtime/helpers/typeof");
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = Dialog;
var _extends2 = _interopRequireDefault(require("@babel/runtime/helpers/extends"));
var _objectSpread2 = _interopRequireDefault(require("@babel/runtime/helpers/objectSpread2"));
var _slicedToArray2 = _interopRequireDefault(require("@babel/runtime/helpers/slicedToArray"));
var _classnames = _interopRequireDefault(require("classnames"));
var _contains = _interopRequireDefault(require("rc-util/lib/Dom/contains"));
var _useId = _interopRequireDefault(require("rc-util/lib/hooks/useId"));
var _KeyCode = _interopRequireDefault(require("rc-util/lib/KeyCode"));
var _pickAttrs = _interopRequireDefault(require("rc-util/lib/pickAttrs"));
var _react = _interopRequireWildcard(require("react"));
var React = _react;
var _util = require("../util");
var _Content = _interopRequireDefault(require("./Content"));
var _Mask = _interopRequireDefault(require("./Mask"));
var _warning = require("rc-util/lib/warning");
function _getRequireWildcardCache(e) { if ("function" != typeof WeakMap) return null; var r = new WeakMap(), t = new WeakMap(); return (_getRequireWildcardCache = function _getRequireWildcardCache(e) { return e ? t : r; })(e); }
function _interopRequireWildcard(e, r) { if (!r && e && e.__esModule) return e; if (null === e || "object" != _typeof(e) && "function" != typeof e) return { default: e }; var t = _getRequireWildcardCache(r); if (t && t.has(e)) return t.get(e); var n = { __proto__: null }, a = Object.defineProperty && Object.getOwnPropertyDescriptor; for (var u in e) if ("default" !== u && Object.prototype.hasOwnProperty.call(e, u)) { var i = a ? Object.getOwnPropertyDescriptor(e, u) : null; i && (i.get || i.set) ? Object.defineProperty(n, u, i) : n[u] = e[u]; } return n.default = e, t && t.set(e, n), n; }
function Dialog(props) {
  var _props$prefixCls = props.prefixCls,
    prefixCls = _props$prefixCls === void 0 ? 'rc-dialog' : _props$prefixCls,
    zIndex = props.zIndex,
    _props$visible = props.visible,
    visible = _props$visible === void 0 ? false : _props$visible,
    _props$keyboard = props.keyboard,
    keyboard = _props$keyboard === void 0 ? true : _props$keyboard,
    _props$focusTriggerAf = props.focusTriggerAfterClose,
    focusTriggerAfterClose = _props$focusTriggerAf === void 0 ? true : _props$focusTriggerAf,
    wrapStyle = props.wrapStyle,
    wrapClassName = props.wrapClassName,
    wrapProps = props.wrapProps,
    onClose = props.onClose,
    afterOpenChange = props.afterOpenChange,
    afterClose = props.afterClose,
    transitionName = props.transitionName,
    animation = props.animation,
    _props$closable = props.closable,
    closable = _props$closable === void 0 ? true : _props$closable,
    _props$mask = props.mask,
    mask = _props$mask === void 0 ? true : _props$mask,
    maskTransitionName = props.maskTransitionName,
    maskAnimation = props.maskAnimation,
    _props$maskClosable = props.maskClosable,
    maskClosable = _props$maskClosable === void 0 ? true : _props$maskClosable,
    maskStyle = props.maskStyle,
    maskProps = props.maskProps,
    rootClassName = props.rootClassName,
    modalClassNames = props.classNames,
    modalStyles = props.styles;
  if (process.env.NODE_ENV !== 'production') {
    ["wrapStyle", "bodyStyle", "maskStyle"].forEach(function (prop) {
      // (prop in props) && console.error(`Warning: ${prop} is deprecated, please use styles instead.`)
      (0, _warning.warning)(!(prop in props), "".concat(prop, " is deprecated, please use styles instead."));
    });
    if ("wrapClassName" in props) {
      (0, _warning.warning)(false, "wrapClassName is deprecated, please use classNames instead.");
    }
  }
  var lastOutSideActiveElementRef = (0, _react.useRef)();
  var wrapperRef = (0, _react.useRef)();
  var contentRef = (0, _react.useRef)();
  var _React$useState = React.useState(visible),
    _React$useState2 = (0, _slicedToArray2.default)(_React$useState, 2),
    animatedVisible = _React$useState2[0],
    setAnimatedVisible = _React$useState2[1];

  // ========================== Init ==========================
  var ariaId = (0, _useId.default)();
  function saveLastOutSideActiveElementRef() {
    if (!(0, _contains.default)(wrapperRef.current, document.activeElement)) {
      lastOutSideActiveElementRef.current = document.activeElement;
    }
  }
  function focusDialogContent() {
    if (!(0, _contains.default)(wrapperRef.current, document.activeElement)) {
      var _contentRef$current;
      (_contentRef$current = contentRef.current) === null || _contentRef$current === void 0 || _contentRef$current.focus();
    }
  }

  // ========================= Events =========================
  function onDialogVisibleChanged(newVisible) {
    // Try to focus
    if (newVisible) {
      focusDialogContent();
    } else {
      // Clean up scroll bar & focus back
      setAnimatedVisible(false);
      if (mask && lastOutSideActiveElementRef.current && focusTriggerAfterClose) {
        try {
          lastOutSideActiveElementRef.current.focus({
            preventScroll: true
          });
        } catch (e) {
          // Do nothing
        }
        lastOutSideActiveElementRef.current = null;
      }

      // Trigger afterClose only when change visible from true to false
      if (animatedVisible) {
        afterClose === null || afterClose === void 0 || afterClose();
      }
    }
    afterOpenChange === null || afterOpenChange === void 0 || afterOpenChange(newVisible);
  }
  function onInternalClose(e) {
    onClose === null || onClose === void 0 || onClose(e);
  }

  // >>> Content
  var contentClickRef = (0, _react.useRef)(false);
  var contentTimeoutRef = (0, _react.useRef)();

  // We need record content click incase content popup out of dialog
  var onContentMouseDown = function onContentMouseDown() {
    clearTimeout(contentTimeoutRef.current);
    contentClickRef.current = true;
  };
  var onContentMouseUp = function onContentMouseUp() {
    contentTimeoutRef.current = setTimeout(function () {
      contentClickRef.current = false;
    });
  };

  // >>> Wrapper
  // Close only when element not on dialog
  var onWrapperClick = null;
  if (maskClosable) {
    onWrapperClick = function onWrapperClick(e) {
      if (contentClickRef.current) {
        contentClickRef.current = false;
      } else if (wrapperRef.current === e.target) {
        onInternalClose(e);
      }
    };
  }
  function onWrapperKeyDown(e) {
    if (keyboard && e.keyCode === _KeyCode.default.ESC) {
      e.stopPropagation();
      onInternalClose(e);
      return;
    }

    // keep focus inside dialog
    if (visible) {
      if (e.keyCode === _KeyCode.default.TAB) {
        contentRef.current.changeActive(!e.shiftKey);
      }
    }
  }

  // ========================= Effect =========================
  (0, _react.useEffect)(function () {
    if (visible) {
      setAnimatedVisible(true);
      saveLastOutSideActiveElementRef();
    }
  }, [visible]);

  // Remove direct should also check the scroll bar update
  (0, _react.useEffect)(function () {
    return function () {
      clearTimeout(contentTimeoutRef.current);
    };
  }, []);

  // ========================= Render =========================
  return /*#__PURE__*/React.createElement("div", (0, _extends2.default)({
    className: (0, _classnames.default)("".concat(prefixCls, "-root"), rootClassName)
  }, (0, _pickAttrs.default)(props, {
    data: true
  })), /*#__PURE__*/React.createElement(_Mask.default, {
    prefixCls: prefixCls,
    visible: mask && visible,
    motionName: (0, _util.getMotionName)(prefixCls, maskTransitionName, maskAnimation),
    style: (0, _objectSpread2.default)((0, _objectSpread2.default)({
      zIndex: zIndex
    }, maskStyle), modalStyles === null || modalStyles === void 0 ? void 0 : modalStyles.mask),
    maskProps: maskProps,
    className: modalClassNames === null || modalClassNames === void 0 ? void 0 : modalClassNames.mask
  }), /*#__PURE__*/React.createElement("div", (0, _extends2.default)({
    tabIndex: -1,
    onKeyDown: onWrapperKeyDown,
    className: (0, _classnames.default)("".concat(prefixCls, "-wrap"), wrapClassName, modalClassNames === null || modalClassNames === void 0 ? void 0 : modalClassNames.wrapper),
    ref: wrapperRef,
    onClick: onWrapperClick,
    style: (0, _objectSpread2.default)((0, _objectSpread2.default)((0, _objectSpread2.default)({
      zIndex: zIndex
    }, wrapStyle), modalStyles === null || modalStyles === void 0 ? void 0 : modalStyles.wrapper), {}, {
      display: !animatedVisible ? 'none' : null
    })
  }, wrapProps), /*#__PURE__*/React.createElement(_Content.default, (0, _extends2.default)({}, props, {
    onMouseDown: onContentMouseDown,
    onMouseUp: onContentMouseUp,
    ref: contentRef,
    closable: closable,
    ariaId: ariaId,
    prefixCls: prefixCls,
    visible: visible && animatedVisible,
    onClose: onInternalClose,
    onVisibleChanged: onDialogVisibleChanged,
    motionName: (0, _util.getMotionName)(prefixCls, transitionName, animation)
  }))));
}