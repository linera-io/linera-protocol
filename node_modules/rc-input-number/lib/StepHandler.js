"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault");
var _typeof = require("@babel/runtime/helpers/typeof");
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = StepHandler;
var _extends2 = _interopRequireDefault(require("@babel/runtime/helpers/extends"));
var _defineProperty2 = _interopRequireDefault(require("@babel/runtime/helpers/defineProperty"));
var React = _interopRequireWildcard(require("react"));
var _classnames = _interopRequireDefault(require("classnames"));
var _useMobile = _interopRequireDefault(require("rc-util/lib/hooks/useMobile"));
var _raf = _interopRequireDefault(require("rc-util/lib/raf"));
function _getRequireWildcardCache(e) { if ("function" != typeof WeakMap) return null; var r = new WeakMap(), t = new WeakMap(); return (_getRequireWildcardCache = function _getRequireWildcardCache(e) { return e ? t : r; })(e); }
function _interopRequireWildcard(e, r) { if (!r && e && e.__esModule) return e; if (null === e || "object" != _typeof(e) && "function" != typeof e) return { default: e }; var t = _getRequireWildcardCache(r); if (t && t.has(e)) return t.get(e); var n = { __proto__: null }, a = Object.defineProperty && Object.getOwnPropertyDescriptor; for (var u in e) if ("default" !== u && Object.prototype.hasOwnProperty.call(e, u)) { var i = a ? Object.getOwnPropertyDescriptor(e, u) : null; i && (i.get || i.set) ? Object.defineProperty(n, u, i) : n[u] = e[u]; } return n.default = e, t && t.set(e, n), n; }
/* eslint-disable react/no-unknown-property */

/**
 * When click and hold on a button - the speed of auto changing the value.
 */
var STEP_INTERVAL = 200;

/**
 * When click and hold on a button - the delay before auto changing the value.
 */
var STEP_DELAY = 600;
function StepHandler(_ref) {
  var prefixCls = _ref.prefixCls,
    upNode = _ref.upNode,
    downNode = _ref.downNode,
    upDisabled = _ref.upDisabled,
    downDisabled = _ref.downDisabled,
    onStep = _ref.onStep;
  // ======================== Step ========================
  var stepTimeoutRef = React.useRef();
  var frameIds = React.useRef([]);
  var onStepRef = React.useRef();
  onStepRef.current = onStep;
  var onStopStep = function onStopStep() {
    clearTimeout(stepTimeoutRef.current);
  };

  // We will interval update step when hold mouse down
  var onStepMouseDown = function onStepMouseDown(e, up) {
    e.preventDefault();
    onStopStep();
    onStepRef.current(up);

    // Loop step for interval
    function loopStep() {
      onStepRef.current(up);
      stepTimeoutRef.current = setTimeout(loopStep, STEP_INTERVAL);
    }

    // First time press will wait some time to trigger loop step update
    stepTimeoutRef.current = setTimeout(loopStep, STEP_DELAY);
  };
  React.useEffect(function () {
    return function () {
      onStopStep();
      frameIds.current.forEach(function (id) {
        return _raf.default.cancel(id);
      });
    };
  }, []);

  // ======================= Render =======================
  var isMobile = (0, _useMobile.default)();
  if (isMobile) {
    return null;
  }
  var handlerClassName = "".concat(prefixCls, "-handler");
  var upClassName = (0, _classnames.default)(handlerClassName, "".concat(handlerClassName, "-up"), (0, _defineProperty2.default)({}, "".concat(handlerClassName, "-up-disabled"), upDisabled));
  var downClassName = (0, _classnames.default)(handlerClassName, "".concat(handlerClassName, "-down"), (0, _defineProperty2.default)({}, "".concat(handlerClassName, "-down-disabled"), downDisabled));

  // fix: https://github.com/ant-design/ant-design/issues/43088
  // In Safari, When we fire onmousedown and onmouseup events in quick succession, 
  // there may be a problem that the onmouseup events are executed first, 
  // resulting in a disordered program execution.
  // So, we need to use requestAnimationFrame to ensure that the onmouseup event is executed after the onmousedown event.
  var safeOnStopStep = function safeOnStopStep() {
    return frameIds.current.push((0, _raf.default)(onStopStep));
  };
  var sharedHandlerProps = {
    unselectable: 'on',
    role: 'button',
    onMouseUp: safeOnStopStep,
    onMouseLeave: safeOnStopStep
  };
  return /*#__PURE__*/React.createElement("div", {
    className: "".concat(handlerClassName, "-wrap")
  }, /*#__PURE__*/React.createElement("span", (0, _extends2.default)({}, sharedHandlerProps, {
    onMouseDown: function onMouseDown(e) {
      onStepMouseDown(e, true);
    },
    "aria-label": "Increase Value",
    "aria-disabled": upDisabled,
    className: upClassName
  }), upNode || /*#__PURE__*/React.createElement("span", {
    unselectable: "on",
    className: "".concat(prefixCls, "-handler-up-inner")
  })), /*#__PURE__*/React.createElement("span", (0, _extends2.default)({}, sharedHandlerProps, {
    onMouseDown: function onMouseDown(e) {
      onStepMouseDown(e, false);
    },
    "aria-label": "Decrease Value",
    "aria-disabled": downDisabled,
    className: downClassName
  }), downNode || /*#__PURE__*/React.createElement("span", {
    unselectable: "on",
    className: "".concat(prefixCls, "-handler-down-inner")
  })));
}