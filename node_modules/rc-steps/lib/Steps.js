"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault").default;
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = void 0;
var _extends2 = _interopRequireDefault(require("@babel/runtime/helpers/extends"));
var _objectSpread2 = _interopRequireDefault(require("@babel/runtime/helpers/objectSpread2"));
var _defineProperty2 = _interopRequireDefault(require("@babel/runtime/helpers/defineProperty"));
var _objectWithoutProperties2 = _interopRequireDefault(require("@babel/runtime/helpers/objectWithoutProperties"));
var _classnames = _interopRequireDefault(require("classnames"));
var _react = _interopRequireDefault(require("react"));
var _Step = _interopRequireDefault(require("./Step"));
var _excluded = ["prefixCls", "style", "className", "children", "direction", "type", "labelPlacement", "iconPrefix", "status", "size", "current", "progressDot", "stepIcon", "initial", "icons", "onChange", "itemRender", "items"];
/* eslint react/no-did-mount-set-state: 0, react/prop-types: 0 */
function Steps(props) {
  var _classNames;
  var _props$prefixCls = props.prefixCls,
    prefixCls = _props$prefixCls === void 0 ? 'rc-steps' : _props$prefixCls,
    _props$style = props.style,
    style = _props$style === void 0 ? {} : _props$style,
    className = props.className,
    children = props.children,
    _props$direction = props.direction,
    direction = _props$direction === void 0 ? 'horizontal' : _props$direction,
    _props$type = props.type,
    type = _props$type === void 0 ? 'default' : _props$type,
    _props$labelPlacement = props.labelPlacement,
    labelPlacement = _props$labelPlacement === void 0 ? 'horizontal' : _props$labelPlacement,
    _props$iconPrefix = props.iconPrefix,
    iconPrefix = _props$iconPrefix === void 0 ? 'rc' : _props$iconPrefix,
    _props$status = props.status,
    status = _props$status === void 0 ? 'process' : _props$status,
    size = props.size,
    _props$current = props.current,
    current = _props$current === void 0 ? 0 : _props$current,
    _props$progressDot = props.progressDot,
    progressDot = _props$progressDot === void 0 ? false : _props$progressDot,
    stepIcon = props.stepIcon,
    _props$initial = props.initial,
    initial = _props$initial === void 0 ? 0 : _props$initial,
    icons = props.icons,
    onChange = props.onChange,
    itemRender = props.itemRender,
    _props$items = props.items,
    items = _props$items === void 0 ? [] : _props$items,
    restProps = (0, _objectWithoutProperties2.default)(props, _excluded);
  var isNav = type === 'navigation';
  var isInline = type === 'inline';

  // inline type requires fixed progressDot direction size.
  var mergedProgressDot = isInline || progressDot;
  var mergedDirection = isInline ? 'horizontal' : direction;
  var mergedSize = isInline ? undefined : size;
  var adjustedLabelPlacement = mergedProgressDot ? 'vertical' : labelPlacement;
  var classString = (0, _classnames.default)(prefixCls, "".concat(prefixCls, "-").concat(mergedDirection), className, (_classNames = {}, (0, _defineProperty2.default)(_classNames, "".concat(prefixCls, "-").concat(mergedSize), mergedSize), (0, _defineProperty2.default)(_classNames, "".concat(prefixCls, "-label-").concat(adjustedLabelPlacement), mergedDirection === 'horizontal'), (0, _defineProperty2.default)(_classNames, "".concat(prefixCls, "-dot"), !!mergedProgressDot), (0, _defineProperty2.default)(_classNames, "".concat(prefixCls, "-navigation"), isNav), (0, _defineProperty2.default)(_classNames, "".concat(prefixCls, "-inline"), isInline), _classNames));
  var onStepClick = function onStepClick(next) {
    if (onChange && current !== next) {
      onChange(next);
    }
  };
  var renderStep = function renderStep(item, index) {
    var mergedItem = (0, _objectSpread2.default)({}, item);
    var stepNumber = initial + index;
    // fix tail color
    if (status === 'error' && index === current - 1) {
      mergedItem.className = "".concat(prefixCls, "-next-error");
    }
    if (!mergedItem.status) {
      if (stepNumber === current) {
        mergedItem.status = status;
      } else if (stepNumber < current) {
        mergedItem.status = 'finish';
      } else {
        mergedItem.status = 'wait';
      }
    }
    if (isInline) {
      mergedItem.icon = undefined;
      mergedItem.subTitle = undefined;
    }
    if (!mergedItem.render && itemRender) {
      mergedItem.render = function (stepItem) {
        return itemRender(mergedItem, stepItem);
      };
    }
    return /*#__PURE__*/_react.default.createElement(_Step.default, (0, _extends2.default)({}, mergedItem, {
      active: stepNumber === current,
      stepNumber: stepNumber + 1,
      stepIndex: stepNumber,
      key: stepNumber,
      prefixCls: prefixCls,
      iconPrefix: iconPrefix,
      wrapperStyle: style,
      progressDot: mergedProgressDot,
      stepIcon: stepIcon,
      icons: icons,
      onStepClick: onChange && onStepClick
    }));
  };
  return /*#__PURE__*/_react.default.createElement("div", (0, _extends2.default)({
    className: classString,
    style: style
  }, restProps), items.filter(function (item) {
    return item;
  }).map(renderStep));
}
Steps.Step = _Step.default;
var _default = Steps;
exports.default = _default;