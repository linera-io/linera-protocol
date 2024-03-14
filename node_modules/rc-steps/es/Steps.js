import _extends from "@babel/runtime/helpers/esm/extends";
import _objectSpread from "@babel/runtime/helpers/esm/objectSpread2";
import _defineProperty from "@babel/runtime/helpers/esm/defineProperty";
import _objectWithoutProperties from "@babel/runtime/helpers/esm/objectWithoutProperties";
var _excluded = ["prefixCls", "style", "className", "children", "direction", "type", "labelPlacement", "iconPrefix", "status", "size", "current", "progressDot", "stepIcon", "initial", "icons", "onChange", "itemRender", "items"];
/* eslint react/no-did-mount-set-state: 0, react/prop-types: 0 */
import classNames from 'classnames';
import React from 'react';
import Step from "./Step";
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
    restProps = _objectWithoutProperties(props, _excluded);
  var isNav = type === 'navigation';
  var isInline = type === 'inline';

  // inline type requires fixed progressDot direction size.
  var mergedProgressDot = isInline || progressDot;
  var mergedDirection = isInline ? 'horizontal' : direction;
  var mergedSize = isInline ? undefined : size;
  var adjustedLabelPlacement = mergedProgressDot ? 'vertical' : labelPlacement;
  var classString = classNames(prefixCls, "".concat(prefixCls, "-").concat(mergedDirection), className, (_classNames = {}, _defineProperty(_classNames, "".concat(prefixCls, "-").concat(mergedSize), mergedSize), _defineProperty(_classNames, "".concat(prefixCls, "-label-").concat(adjustedLabelPlacement), mergedDirection === 'horizontal'), _defineProperty(_classNames, "".concat(prefixCls, "-dot"), !!mergedProgressDot), _defineProperty(_classNames, "".concat(prefixCls, "-navigation"), isNav), _defineProperty(_classNames, "".concat(prefixCls, "-inline"), isInline), _classNames));
  var onStepClick = function onStepClick(next) {
    if (onChange && current !== next) {
      onChange(next);
    }
  };
  var renderStep = function renderStep(item, index) {
    var mergedItem = _objectSpread({}, item);
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
    return /*#__PURE__*/React.createElement(Step, _extends({}, mergedItem, {
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
  return /*#__PURE__*/React.createElement("div", _extends({
    className: classString,
    style: style
  }, restProps), items.filter(function (item) {
    return item;
  }).map(renderStep));
}
Steps.Step = Step;
export default Steps;