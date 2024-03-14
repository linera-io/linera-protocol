"use strict";
"use client";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault").default;
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = void 0;
var _react = _interopRequireDefault(require("react"));
var _classnames = _interopRequireDefault(require("classnames"));
var _rcResizeObserver = _interopRequireDefault(require("rc-resize-observer"));
var _omit = _interopRequireDefault(require("rc-util/lib/omit"));
var _throttleByAnimationFrame = _interopRequireDefault(require("../_util/throttleByAnimationFrame"));
var _configProvider = require("../config-provider");
var _style = _interopRequireDefault(require("./style"));
var _utils = require("./utils");
const TRIGGER_EVENTS = ['resize', 'scroll', 'touchstart', 'touchmove', 'touchend', 'pageshow', 'load'];
function getDefaultTarget() {
  return typeof window !== 'undefined' ? window : null;
}
var AffixStatus;
(function (AffixStatus) {
  AffixStatus[AffixStatus["None"] = 0] = "None";
  AffixStatus[AffixStatus["Prepare"] = 1] = "Prepare";
})(AffixStatus || (AffixStatus = {}));
const Affix = /*#__PURE__*/_react.default.forwardRef((props, ref) => {
  var _a;
  const {
    style,
    offsetTop,
    offsetBottom,
    prefixCls,
    className,
    rootClassName,
    children,
    target,
    onChange
  } = props;
  const {
    getPrefixCls,
    getTargetContainer
  } = _react.default.useContext(_configProvider.ConfigContext);
  const affixPrefixCls = getPrefixCls('affix', prefixCls);
  const [lastAffix, setLastAffix] = _react.default.useState(false);
  const [affixStyle, setAffixStyle] = _react.default.useState();
  const [placeholderStyle, setPlaceholderStyle] = _react.default.useState();
  const status = _react.default.useRef(AffixStatus.None);
  const prevTarget = _react.default.useRef(null);
  const prevListener = _react.default.useRef();
  const placeholderNodeRef = _react.default.useRef(null);
  const fixedNodeRef = _react.default.useRef(null);
  const timer = _react.default.useRef(null);
  const targetFunc = (_a = target !== null && target !== void 0 ? target : getTargetContainer) !== null && _a !== void 0 ? _a : getDefaultTarget;
  const internalOffsetTop = offsetBottom === undefined && offsetTop === undefined ? 0 : offsetTop;
  // =================== Measure ===================
  const measure = () => {
    if (status.current !== AffixStatus.Prepare || !fixedNodeRef.current || !placeholderNodeRef.current || !targetFunc) {
      return;
    }
    const targetNode = targetFunc();
    if (targetNode) {
      const newState = {
        status: AffixStatus.None
      };
      const placeholderRect = (0, _utils.getTargetRect)(placeholderNodeRef.current);
      if (placeholderRect.top === 0 && placeholderRect.left === 0 && placeholderRect.width === 0 && placeholderRect.height === 0) {
        return;
      }
      const targetRect = (0, _utils.getTargetRect)(targetNode);
      const fixedTop = (0, _utils.getFixedTop)(placeholderRect, targetRect, internalOffsetTop);
      const fixedBottom = (0, _utils.getFixedBottom)(placeholderRect, targetRect, offsetBottom);
      if (fixedTop !== undefined) {
        newState.affixStyle = {
          position: 'fixed',
          top: fixedTop,
          width: placeholderRect.width,
          height: placeholderRect.height
        };
        newState.placeholderStyle = {
          width: placeholderRect.width,
          height: placeholderRect.height
        };
      } else if (fixedBottom !== undefined) {
        newState.affixStyle = {
          position: 'fixed',
          bottom: fixedBottom,
          width: placeholderRect.width,
          height: placeholderRect.height
        };
        newState.placeholderStyle = {
          width: placeholderRect.width,
          height: placeholderRect.height
        };
      }
      newState.lastAffix = !!newState.affixStyle;
      if (lastAffix !== newState.lastAffix) {
        onChange === null || onChange === void 0 ? void 0 : onChange(newState.lastAffix);
      }
      status.current = newState.status;
      setAffixStyle(newState.affixStyle);
      setPlaceholderStyle(newState.placeholderStyle);
      setLastAffix(newState.lastAffix);
    }
  };
  const prepareMeasure = () => {
    var _a;
    status.current = AffixStatus.Prepare;
    measure();
    if (process.env.NODE_ENV === 'test') {
      (_a = props === null || props === void 0 ? void 0 : props.onTestUpdatePosition) === null || _a === void 0 ? void 0 : _a.call(props);
    }
  };
  const updatePosition = (0, _throttleByAnimationFrame.default)(() => {
    prepareMeasure();
  });
  const lazyUpdatePosition = (0, _throttleByAnimationFrame.default)(() => {
    // Check position change before measure to make Safari smooth
    if (targetFunc && affixStyle) {
      const targetNode = targetFunc();
      if (targetNode && placeholderNodeRef.current) {
        const targetRect = (0, _utils.getTargetRect)(targetNode);
        const placeholderRect = (0, _utils.getTargetRect)(placeholderNodeRef.current);
        const fixedTop = (0, _utils.getFixedTop)(placeholderRect, targetRect, internalOffsetTop);
        const fixedBottom = (0, _utils.getFixedBottom)(placeholderRect, targetRect, offsetBottom);
        if (fixedTop !== undefined && affixStyle.top === fixedTop || fixedBottom !== undefined && affixStyle.bottom === fixedBottom) {
          return;
        }
      }
    }
    // Directly call prepare measure since it's already throttled.
    prepareMeasure();
  });
  const addListeners = () => {
    const listenerTarget = targetFunc === null || targetFunc === void 0 ? void 0 : targetFunc();
    if (!listenerTarget) {
      return;
    }
    TRIGGER_EVENTS.forEach(eventName => {
      var _a;
      if (prevListener.current) {
        (_a = prevTarget.current) === null || _a === void 0 ? void 0 : _a.removeEventListener(eventName, prevListener.current);
      }
      listenerTarget === null || listenerTarget === void 0 ? void 0 : listenerTarget.addEventListener(eventName, lazyUpdatePosition);
    });
    prevTarget.current = listenerTarget;
    prevListener.current = lazyUpdatePosition;
  };
  const removeListeners = () => {
    if (timer.current) {
      clearTimeout(timer.current);
      timer.current = null;
    }
    const newTarget = targetFunc === null || targetFunc === void 0 ? void 0 : targetFunc();
    TRIGGER_EVENTS.forEach(eventName => {
      var _a;
      newTarget === null || newTarget === void 0 ? void 0 : newTarget.removeEventListener(eventName, lazyUpdatePosition);
      if (prevListener.current) {
        (_a = prevTarget.current) === null || _a === void 0 ? void 0 : _a.removeEventListener(eventName, prevListener.current);
      }
    });
    updatePosition.cancel();
    lazyUpdatePosition.cancel();
  };
  _react.default.useImperativeHandle(ref, () => ({
    updatePosition
  }));
  // mount & unmount
  _react.default.useEffect(() => {
    // [Legacy] Wait for parent component ref has its value.
    // We should use target as directly element instead of function which makes element check hard.
    timer.current = setTimeout(addListeners);
    return () => removeListeners();
  }, []);
  _react.default.useEffect(() => {
    addListeners();
  }, [target, affixStyle]);
  _react.default.useEffect(() => {
    updatePosition();
  }, [target, offsetTop, offsetBottom]);
  const [wrapCSSVar, hashId, cssVarCls] = (0, _style.default)(affixPrefixCls);
  const rootCls = (0, _classnames.default)(rootClassName, hashId, affixPrefixCls, cssVarCls);
  const mergedCls = (0, _classnames.default)({
    [rootCls]: affixStyle
  });
  let otherProps = (0, _omit.default)(props, ['prefixCls', 'offsetTop', 'offsetBottom', 'target', 'onChange', 'rootClassName']);
  if (process.env.NODE_ENV === 'test') {
    otherProps = (0, _omit.default)(otherProps, ['onTestUpdatePosition']);
  }
  return wrapCSSVar( /*#__PURE__*/_react.default.createElement(_rcResizeObserver.default, {
    onResize: updatePosition
  }, /*#__PURE__*/_react.default.createElement("div", Object.assign({
    style: style,
    className: className,
    ref: placeholderNodeRef
  }, otherProps), affixStyle && /*#__PURE__*/_react.default.createElement("div", {
    style: placeholderStyle,
    "aria-hidden": "true"
  }), /*#__PURE__*/_react.default.createElement("div", {
    className: mergedCls,
    ref: fixedNodeRef,
    style: affixStyle
  }, /*#__PURE__*/_react.default.createElement(_rcResizeObserver.default, {
    onResize: updatePosition
  }, children)))));
});
if (process.env.NODE_ENV !== 'production') {
  Affix.displayName = 'Affix';
}
var _default = exports.default = Affix;