"use strict";
"use client";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault").default;
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = void 0;
var _react = _interopRequireDefault(require("react"));
var _classnames = _interopRequireDefault(require("classnames"));
var _rcSlider = _interopRequireDefault(require("rc-slider"));
var _warning = require("../_util/warning");
var _configProvider = require("../config-provider");
var _DisabledContext = _interopRequireDefault(require("../config-provider/DisabledContext"));
var _SliderTooltip = _interopRequireDefault(require("./SliderTooltip"));
var _style = _interopRequireDefault(require("./style"));
var __rest = void 0 && (void 0).__rest || function (s, e) {
  var t = {};
  for (var p in s) if (Object.prototype.hasOwnProperty.call(s, p) && e.indexOf(p) < 0) t[p] = s[p];
  if (s != null && typeof Object.getOwnPropertySymbols === "function") for (var i = 0, p = Object.getOwnPropertySymbols(s); i < p.length; i++) {
    if (e.indexOf(p[i]) < 0 && Object.prototype.propertyIsEnumerable.call(s, p[i])) t[p[i]] = s[p[i]];
  }
  return t;
};
function getTipFormatter(tipFormatter, legacyTipFormatter) {
  if (tipFormatter || tipFormatter === null) {
    return tipFormatter;
  }
  if (legacyTipFormatter || legacyTipFormatter === null) {
    return legacyTipFormatter;
  }
  return val => typeof val === 'number' ? val.toString() : '';
}
const Slider = /*#__PURE__*/_react.default.forwardRef((props, ref) => {
  const {
      prefixCls: customizePrefixCls,
      range,
      className,
      rootClassName,
      style,
      disabled,
      // Deprecated Props
      tooltipPrefixCls: legacyTooltipPrefixCls,
      tipFormatter: legacyTipFormatter,
      tooltipVisible: legacyTooltipVisible,
      getTooltipPopupContainer: legacyGetTooltipPopupContainer,
      tooltipPlacement: legacyTooltipPlacement
    } = props,
    restProps = __rest(props, ["prefixCls", "range", "className", "rootClassName", "style", "disabled", "tooltipPrefixCls", "tipFormatter", "tooltipVisible", "getTooltipPopupContainer", "tooltipPlacement"]);
  const {
    direction,
    slider,
    getPrefixCls,
    getPopupContainer
  } = _react.default.useContext(_configProvider.ConfigContext);
  const contextDisabled = _react.default.useContext(_DisabledContext.default);
  const mergedDisabled = disabled !== null && disabled !== void 0 ? disabled : contextDisabled;
  const [opens, setOpens] = _react.default.useState({});
  const toggleTooltipOpen = (index, open) => {
    setOpens(prev => Object.assign(Object.assign({}, prev), {
      [index]: open
    }));
  };
  const getTooltipPlacement = (placement, vertical) => {
    if (placement) {
      return placement;
    }
    if (!vertical) {
      return 'top';
    }
    return direction === 'rtl' ? 'left' : 'right';
  };
  const prefixCls = getPrefixCls('slider', customizePrefixCls);
  const [wrapCSSVar, hashId, cssVarCls] = (0, _style.default)(prefixCls);
  const cls = (0, _classnames.default)(className, slider === null || slider === void 0 ? void 0 : slider.className, rootClassName, {
    [`${prefixCls}-rtl`]: direction === 'rtl'
  }, hashId, cssVarCls);
  // make reverse default on rtl direction
  if (direction === 'rtl' && !restProps.vertical) {
    restProps.reverse = !restProps.reverse;
  }
  // Range config
  const [mergedRange, draggableTrack] = _react.default.useMemo(() => {
    if (!range) {
      return [false];
    }
    return typeof range === 'object' ? [true, range.draggableTrack] : [true, false];
  }, [range]);
  // Warning for deprecated usage
  if (process.env.NODE_ENV !== 'production') {
    const warning = (0, _warning.devUseWarning)('Slider');
    [['tooltipPrefixCls', 'prefixCls'], ['getTooltipPopupContainer', 'getPopupContainer'], ['tipFormatter', 'formatter'], ['tooltipPlacement', 'placement'], ['tooltipVisible', 'open']].forEach(_ref => {
      let [deprecatedName, newName] = _ref;
      warning.deprecated(!(deprecatedName in props), deprecatedName, `tooltip.${newName}`);
    });
  }
  const handleRender = (node, info) => {
    var _a;
    const {
      index,
      dragging
    } = info;
    const {
      tooltip = {},
      vertical
    } = props;
    const tooltipProps = Object.assign({}, tooltip);
    const {
      open: tooltipOpen,
      placement: tooltipPlacement,
      getPopupContainer: getTooltipPopupContainer,
      prefixCls: customizeTooltipPrefixCls,
      formatter: tipFormatter
    } = tooltipProps;
    const mergedTipFormatter = getTipFormatter(tipFormatter, legacyTipFormatter);
    const isTipFormatter = mergedTipFormatter ? opens[index] || dragging : false;
    const open = (_a = tooltipOpen !== null && tooltipOpen !== void 0 ? tooltipOpen : legacyTooltipVisible) !== null && _a !== void 0 ? _a : tooltipOpen === undefined && isTipFormatter;
    const passedProps = Object.assign(Object.assign({}, node.props), {
      onMouseEnter: () => toggleTooltipOpen(index, true),
      onMouseLeave: () => toggleTooltipOpen(index, false),
      onFocus: e => {
        var _a;
        toggleTooltipOpen(index, true);
        (_a = restProps.onFocus) === null || _a === void 0 ? void 0 : _a.call(restProps, e);
      },
      onBlur: e => {
        var _a;
        toggleTooltipOpen(index, false);
        (_a = restProps.onBlur) === null || _a === void 0 ? void 0 : _a.call(restProps, e);
      }
    });
    return /*#__PURE__*/_react.default.createElement(_SliderTooltip.default, Object.assign({}, tooltipProps, {
      prefixCls: getPrefixCls('tooltip', customizeTooltipPrefixCls !== null && customizeTooltipPrefixCls !== void 0 ? customizeTooltipPrefixCls : legacyTooltipPrefixCls),
      title: mergedTipFormatter ? mergedTipFormatter(info.value) : '',
      open: open,
      placement: getTooltipPlacement(tooltipPlacement !== null && tooltipPlacement !== void 0 ? tooltipPlacement : legacyTooltipPlacement, vertical),
      key: index,
      overlayClassName: `${prefixCls}-tooltip`,
      getPopupContainer: getTooltipPopupContainer || legacyGetTooltipPopupContainer || getPopupContainer
    }), /*#__PURE__*/_react.default.cloneElement(node, passedProps));
  };
  const mergedStyle = Object.assign(Object.assign({}, slider === null || slider === void 0 ? void 0 : slider.style), style);
  return wrapCSSVar( /*#__PURE__*/_react.default.createElement(_rcSlider.default, Object.assign({}, restProps, {
    step: restProps.step,
    range: mergedRange,
    draggableTrack: draggableTrack,
    className: cls,
    style: mergedStyle,
    disabled: mergedDisabled,
    ref: ref,
    prefixCls: prefixCls,
    handleRender: handleRender
  })));
});
if (process.env.NODE_ENV !== 'production') {
  Slider.displayName = 'Slider';
}
var _default = exports.default = Slider;