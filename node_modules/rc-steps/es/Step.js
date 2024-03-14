import _extends from "@babel/runtime/helpers/esm/extends";
import _objectSpread from "@babel/runtime/helpers/esm/objectSpread2";
import _defineProperty from "@babel/runtime/helpers/esm/defineProperty";
import _objectWithoutProperties from "@babel/runtime/helpers/esm/objectWithoutProperties";
var _excluded = ["className", "prefixCls", "style", "active", "status", "iconPrefix", "icon", "wrapperStyle", "stepNumber", "disabled", "description", "title", "subTitle", "progressDot", "stepIcon", "tailContent", "icons", "stepIndex", "onStepClick", "onClick", "render"];
/* eslint react/prop-types: 0 */
import * as React from 'react';
import classNames from 'classnames';
import KeyCode from "rc-util/es/KeyCode";
function isString(str) {
  return typeof str === 'string';
}
function Step(props) {
  var _classNames2;
  var className = props.className,
    prefixCls = props.prefixCls,
    style = props.style,
    active = props.active,
    status = props.status,
    iconPrefix = props.iconPrefix,
    icon = props.icon,
    wrapperStyle = props.wrapperStyle,
    stepNumber = props.stepNumber,
    disabled = props.disabled,
    description = props.description,
    title = props.title,
    subTitle = props.subTitle,
    progressDot = props.progressDot,
    stepIcon = props.stepIcon,
    tailContent = props.tailContent,
    icons = props.icons,
    stepIndex = props.stepIndex,
    onStepClick = props.onStepClick,
    onClick = props.onClick,
    render = props.render,
    restProps = _objectWithoutProperties(props, _excluded);

  // ========================= Click ==========================
  var clickable = !!onStepClick && !disabled;
  var accessibilityProps = {};
  if (clickable) {
    accessibilityProps.role = 'button';
    accessibilityProps.tabIndex = 0;
    accessibilityProps.onClick = function (e) {
      onClick === null || onClick === void 0 ? void 0 : onClick(e);
      onStepClick(stepIndex);
    };
    accessibilityProps.onKeyDown = function (e) {
      var which = e.which;
      if (which === KeyCode.ENTER || which === KeyCode.SPACE) {
        onStepClick(stepIndex);
      }
    };
  }

  // ========================= Render =========================
  var renderIconNode = function renderIconNode() {
    var _classNames;
    var iconNode;
    var iconClassName = classNames("".concat(prefixCls, "-icon"), "".concat(iconPrefix, "icon"), (_classNames = {}, _defineProperty(_classNames, "".concat(iconPrefix, "icon-").concat(icon), icon && isString(icon)), _defineProperty(_classNames, "".concat(iconPrefix, "icon-check"), !icon && status === 'finish' && (icons && !icons.finish || !icons)), _defineProperty(_classNames, "".concat(iconPrefix, "icon-cross"), !icon && status === 'error' && (icons && !icons.error || !icons)), _classNames));
    var iconDot = /*#__PURE__*/React.createElement("span", {
      className: "".concat(prefixCls, "-icon-dot")
    });
    // `progressDot` enjoy the highest priority
    if (progressDot) {
      if (typeof progressDot === 'function') {
        iconNode = /*#__PURE__*/React.createElement("span", {
          className: "".concat(prefixCls, "-icon")
        }, progressDot(iconDot, {
          index: stepNumber - 1,
          status: status,
          title: title,
          description: description
        }));
      } else {
        iconNode = /*#__PURE__*/React.createElement("span", {
          className: "".concat(prefixCls, "-icon")
        }, iconDot);
      }
    } else if (icon && !isString(icon)) {
      iconNode = /*#__PURE__*/React.createElement("span", {
        className: "".concat(prefixCls, "-icon")
      }, icon);
    } else if (icons && icons.finish && status === 'finish') {
      iconNode = /*#__PURE__*/React.createElement("span", {
        className: "".concat(prefixCls, "-icon")
      }, icons.finish);
    } else if (icons && icons.error && status === 'error') {
      iconNode = /*#__PURE__*/React.createElement("span", {
        className: "".concat(prefixCls, "-icon")
      }, icons.error);
    } else if (icon || status === 'finish' || status === 'error') {
      iconNode = /*#__PURE__*/React.createElement("span", {
        className: iconClassName
      });
    } else {
      iconNode = /*#__PURE__*/React.createElement("span", {
        className: "".concat(prefixCls, "-icon")
      }, stepNumber);
    }
    if (stepIcon) {
      iconNode = stepIcon({
        index: stepNumber - 1,
        status: status,
        title: title,
        description: description,
        node: iconNode
      });
    }
    return iconNode;
  };
  var mergedStatus = status || 'wait';
  var classString = classNames("".concat(prefixCls, "-item"), "".concat(prefixCls, "-item-").concat(mergedStatus), className, (_classNames2 = {}, _defineProperty(_classNames2, "".concat(prefixCls, "-item-custom"), icon), _defineProperty(_classNames2, "".concat(prefixCls, "-item-active"), active), _defineProperty(_classNames2, "".concat(prefixCls, "-item-disabled"), disabled === true), _classNames2));
  var stepItemStyle = _objectSpread({}, style);
  var stepNode = /*#__PURE__*/React.createElement("div", _extends({}, restProps, {
    className: classString,
    style: stepItemStyle
  }), /*#__PURE__*/React.createElement("div", _extends({
    onClick: onClick
  }, accessibilityProps, {
    className: "".concat(prefixCls, "-item-container")
  }), /*#__PURE__*/React.createElement("div", {
    className: "".concat(prefixCls, "-item-tail")
  }, tailContent), /*#__PURE__*/React.createElement("div", {
    className: "".concat(prefixCls, "-item-icon")
  }, renderIconNode()), /*#__PURE__*/React.createElement("div", {
    className: "".concat(prefixCls, "-item-content")
  }, /*#__PURE__*/React.createElement("div", {
    className: "".concat(prefixCls, "-item-title")
  }, title, subTitle && /*#__PURE__*/React.createElement("div", {
    title: typeof subTitle === 'string' ? subTitle : undefined,
    className: "".concat(prefixCls, "-item-subtitle")
  }, subTitle)), description && /*#__PURE__*/React.createElement("div", {
    className: "".concat(prefixCls, "-item-description")
  }, description))));
  if (render) {
    stepNode = render(stepNode) || null;
  }
  return stepNode;
}
export default Step;