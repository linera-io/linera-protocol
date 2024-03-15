"use strict";
"use client";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault").default;
var _interopRequireWildcard = require("@babel/runtime/helpers/interopRequireWildcard").default;
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = void 0;
var _react = _interopRequireWildcard(require("react"));
var _tour = _interopRequireDefault(require("@rc-component/tour"));
var _classnames = _interopRequireDefault(require("classnames"));
var _useZIndex = require("../_util/hooks/useZIndex");
var _placements = _interopRequireDefault(require("../_util/placements"));
var _zindexContext = _interopRequireDefault(require("../_util/zindexContext"));
var _configProvider = require("../config-provider");
var _internal = require("../theme/internal");
var _panelRender = _interopRequireDefault(require("./panelRender"));
var _PurePanel = _interopRequireDefault(require("./PurePanel"));
var _style = _interopRequireDefault(require("./style"));
var __rest = void 0 && (void 0).__rest || function (s, e) {
  var t = {};
  for (var p in s) if (Object.prototype.hasOwnProperty.call(s, p) && e.indexOf(p) < 0) t[p] = s[p];
  if (s != null && typeof Object.getOwnPropertySymbols === "function") for (var i = 0, p = Object.getOwnPropertySymbols(s); i < p.length; i++) {
    if (e.indexOf(p[i]) < 0 && Object.prototype.propertyIsEnumerable.call(s, p[i])) t[p[i]] = s[p[i]];
  }
  return t;
};
const Tour = props => {
  const {
      prefixCls: customizePrefixCls,
      type,
      rootClassName,
      indicatorsRender,
      steps,
      closeIcon
    } = props,
    restProps = __rest(props, ["prefixCls", "type", "rootClassName", "indicatorsRender", "steps", "closeIcon"]);
  const {
    getPrefixCls,
    direction,
    tour
  } = (0, _react.useContext)(_configProvider.ConfigContext);
  const prefixCls = getPrefixCls('tour', customizePrefixCls);
  const [wrapCSSVar, hashId, cssVarCls] = (0, _style.default)(prefixCls);
  const [, token] = (0, _internal.useToken)();
  const mergedSteps = _react.default.useMemo(() => steps === null || steps === void 0 ? void 0 : steps.map(step => {
    var _a;
    return Object.assign(Object.assign({}, step), {
      className: (0, _classnames.default)(step.className, {
        [`${prefixCls}-primary`]: ((_a = step.type) !== null && _a !== void 0 ? _a : type) === 'primary'
      })
    });
  }), [steps, type]);
  const builtinPlacements = config => {
    var _a;
    return (0, _placements.default)({
      arrowPointAtCenter: (_a = config === null || config === void 0 ? void 0 : config.arrowPointAtCenter) !== null && _a !== void 0 ? _a : true,
      autoAdjustOverflow: true,
      offset: token.marginXXS,
      arrowWidth: token.sizePopupArrow,
      borderRadius: token.borderRadius
    });
  };
  const customClassName = (0, _classnames.default)({
    [`${prefixCls}-rtl`]: direction === 'rtl'
  }, hashId, cssVarCls, rootClassName);
  const mergedRenderPanel = (stepProps, stepCurrent) => ( /*#__PURE__*/_react.default.createElement(_panelRender.default, {
    type: type,
    stepProps: stepProps,
    current: stepCurrent,
    indicatorsRender: indicatorsRender
  }));
  // ============================ zIndex ============================
  const [zIndex, contextZIndex] = (0, _useZIndex.useZIndex)('Tour', restProps.zIndex);
  return wrapCSSVar( /*#__PURE__*/_react.default.createElement(_zindexContext.default.Provider, {
    value: contextZIndex
  }, /*#__PURE__*/_react.default.createElement(_tour.default, Object.assign({}, restProps, {
    closeIcon: closeIcon !== null && closeIcon !== void 0 ? closeIcon : tour === null || tour === void 0 ? void 0 : tour.closeIcon,
    zIndex: zIndex,
    rootClassName: customClassName,
    prefixCls: prefixCls,
    animated: true,
    renderPanel: mergedRenderPanel,
    builtinPlacements: builtinPlacements,
    steps: mergedSteps
  }))));
};
if (process.env.NODE_ENV !== 'production') {
  Tour.displayName = 'Tour';
}
Tour._InternalPanelDoNotUseOrYouWillBeFired = _PurePanel.default;
var _default = exports.default = Tour;