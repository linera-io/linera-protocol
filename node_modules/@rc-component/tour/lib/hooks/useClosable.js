"use strict";

var _interopRequireWildcard = require("@babel/runtime/helpers/interopRequireWildcard").default;
var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault").default;
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.useClosable = useClosable;
var _objectSpread2 = _interopRequireDefault(require("@babel/runtime/helpers/objectSpread2"));
var _typeof2 = _interopRequireDefault(require("@babel/runtime/helpers/typeof"));
var React = _interopRequireWildcard(require("react"));
function isConfigObj(closable) {
  return closable !== null && (0, _typeof2.default)(closable) === 'object';
}
/**
 * Convert `closable` to ClosableConfig.
 * When `preset` is true, will auto fill ClosableConfig with default value.
 */
function getClosableConfig(closable, closeIcon, preset) {
  if (closable === false || closeIcon === false && (!isConfigObj(closable) || !closable.closeIcon)) {
    return null;
  }
  var mergedCloseIcon = typeof closeIcon !== 'boolean' ? closeIcon : undefined;
  if (isConfigObj(closable)) {
    var _closable$closeIcon;
    return (0, _objectSpread2.default)((0, _objectSpread2.default)({}, closable), {}, {
      closeIcon: (_closable$closeIcon = closable.closeIcon) !== null && _closable$closeIcon !== void 0 ? _closable$closeIcon : mergedCloseIcon
    });
  }

  // When StepClosable no need auto fill, but RootClosable need this.
  return preset || closable || closeIcon ? {
    closeIcon: mergedCloseIcon
  } : 'empty';
}
function useClosable(stepClosable, stepCloseIcon, closable, closeIcon) {
  return React.useMemo(function () {
    var stepClosableConfig = getClosableConfig(stepClosable, stepCloseIcon, false);
    var rootClosableConfig = getClosableConfig(closable, closeIcon, true);
    if (stepClosableConfig !== 'empty') {
      return stepClosableConfig;
    }
    return rootClosableConfig;
  }, [closable, closeIcon, stepClosable, stepCloseIcon]);
}