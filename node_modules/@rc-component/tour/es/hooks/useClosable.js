import _objectSpread from "@babel/runtime/helpers/esm/objectSpread2";
import _typeof from "@babel/runtime/helpers/esm/typeof";
import * as React from 'react';
function isConfigObj(closable) {
  return closable !== null && _typeof(closable) === 'object';
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
    return _objectSpread(_objectSpread({}, closable), {}, {
      closeIcon: (_closable$closeIcon = closable.closeIcon) !== null && _closable$closeIcon !== void 0 ? _closable$closeIcon : mergedCloseIcon
    });
  }

  // When StepClosable no need auto fill, but RootClosable need this.
  return preset || closable || closeIcon ? {
    closeIcon: mergedCloseIcon
  } : 'empty';
}
export function useClosable(stepClosable, stepCloseIcon, closable, closeIcon) {
  return React.useMemo(function () {
    var stepClosableConfig = getClosableConfig(stepClosable, stepCloseIcon, false);
    var rootClosableConfig = getClosableConfig(closable, closeIcon, true);
    if (stepClosableConfig !== 'empty') {
      return stepClosableConfig;
    }
    return rootClosableConfig;
  }, [closable, closeIcon, stepClosable, stepCloseIcon]);
}