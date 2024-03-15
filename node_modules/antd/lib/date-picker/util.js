"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault").default;
var _interopRequireWildcard = require("@babel/runtime/helpers/interopRequireWildcard").default;
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.getPlaceholder = getPlaceholder;
exports.getRangePlaceholder = getRangePlaceholder;
exports.transPlacement2DropdownAlign = transPlacement2DropdownAlign;
exports.useIcons = useIcons;
var React = _interopRequireWildcard(require("react"));
var _useIcons = _interopRequireDefault(require("../select/useIcons"));
function getPlaceholder(locale, picker, customizePlaceholder) {
  if (customizePlaceholder !== undefined) {
    return customizePlaceholder;
  }
  if (picker === 'year' && locale.lang.yearPlaceholder) {
    return locale.lang.yearPlaceholder;
  }
  if (picker === 'quarter' && locale.lang.quarterPlaceholder) {
    return locale.lang.quarterPlaceholder;
  }
  if (picker === 'month' && locale.lang.monthPlaceholder) {
    return locale.lang.monthPlaceholder;
  }
  if (picker === 'week' && locale.lang.weekPlaceholder) {
    return locale.lang.weekPlaceholder;
  }
  if (picker === 'time' && locale.timePickerLocale.placeholder) {
    return locale.timePickerLocale.placeholder;
  }
  return locale.lang.placeholder;
}
function getRangePlaceholder(locale, picker, customizePlaceholder) {
  if (customizePlaceholder !== undefined) {
    return customizePlaceholder;
  }
  if (picker === 'year' && locale.lang.yearPlaceholder) {
    return locale.lang.rangeYearPlaceholder;
  }
  if (picker === 'quarter' && locale.lang.quarterPlaceholder) {
    return locale.lang.rangeQuarterPlaceholder;
  }
  if (picker === 'month' && locale.lang.monthPlaceholder) {
    return locale.lang.rangeMonthPlaceholder;
  }
  if (picker === 'week' && locale.lang.weekPlaceholder) {
    return locale.lang.rangeWeekPlaceholder;
  }
  if (picker === 'time' && locale.timePickerLocale.placeholder) {
    return locale.timePickerLocale.rangePlaceholder;
  }
  return locale.lang.rangePlaceholder;
}
function transPlacement2DropdownAlign(direction, placement) {
  const overflow = {
    adjustX: 1,
    adjustY: 1
  };
  switch (placement) {
    case 'bottomLeft':
      {
        return {
          points: ['tl', 'bl'],
          offset: [0, 4],
          overflow
        };
      }
    case 'bottomRight':
      {
        return {
          points: ['tr', 'br'],
          offset: [0, 4],
          overflow
        };
      }
    case 'topLeft':
      {
        return {
          points: ['bl', 'tl'],
          offset: [0, -4],
          overflow
        };
      }
    case 'topRight':
      {
        return {
          points: ['br', 'tr'],
          offset: [0, -4],
          overflow
        };
      }
    default:
      {
        return {
          points: direction === 'rtl' ? ['tr', 'br'] : ['tl', 'bl'],
          offset: [0, 4],
          overflow
        };
      }
  }
}
function useIcons(props, prefixCls) {
  const {
    allowClear = true
  } = props;
  const {
    clearIcon,
    removeIcon
  } = (0, _useIcons.default)(Object.assign(Object.assign({}, props), {
    prefixCls,
    componentName: 'DatePicker'
  }));
  const mergedAllowClear = React.useMemo(() => {
    if (allowClear === false) {
      return false;
    }
    const allowClearConfig = allowClear === true ? {} : allowClear;
    return Object.assign({
      clearIcon: clearIcon
    }, allowClearConfig);
  }, [allowClear, clearIcon]);
  return [mergedAllowClear, removeIcon];
}