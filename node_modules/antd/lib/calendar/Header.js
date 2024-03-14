"use strict";
"use client";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault").default;
var _interopRequireWildcard = require("@babel/runtime/helpers/interopRequireWildcard").default;
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = void 0;
var _react = _interopRequireWildcard(require("react"));
var React = _react;
var _context = require("../form/context");
var _radio = require("../radio");
var _select = _interopRequireDefault(require("../select"));
const YearSelectOffset = 10;
const YearSelectTotal = 20;
function YearSelect(props) {
  const {
    fullscreen,
    validRange,
    generateConfig,
    locale,
    prefixCls,
    value,
    onChange,
    divRef
  } = props;
  const year = generateConfig.getYear(value || generateConfig.getNow());
  let start = year - YearSelectOffset;
  let end = start + YearSelectTotal;
  if (validRange) {
    start = generateConfig.getYear(validRange[0]);
    end = generateConfig.getYear(validRange[1]) + 1;
  }
  const suffix = locale && locale.year === '年' ? '年' : '';
  const options = [];
  for (let index = start; index < end; index++) {
    options.push({
      label: `${index}${suffix}`,
      value: index
    });
  }
  return /*#__PURE__*/React.createElement(_select.default, {
    size: fullscreen ? undefined : 'small',
    options: options,
    value: year,
    className: `${prefixCls}-year-select`,
    onChange: numYear => {
      let newDate = generateConfig.setYear(value, numYear);
      if (validRange) {
        const [startDate, endDate] = validRange;
        const newYear = generateConfig.getYear(newDate);
        const newMonth = generateConfig.getMonth(newDate);
        if (newYear === generateConfig.getYear(endDate) && newMonth > generateConfig.getMonth(endDate)) {
          newDate = generateConfig.setMonth(newDate, generateConfig.getMonth(endDate));
        }
        if (newYear === generateConfig.getYear(startDate) && newMonth < generateConfig.getMonth(startDate)) {
          newDate = generateConfig.setMonth(newDate, generateConfig.getMonth(startDate));
        }
      }
      onChange(newDate);
    },
    getPopupContainer: () => divRef.current
  });
}
function MonthSelect(props) {
  const {
    prefixCls,
    fullscreen,
    validRange,
    value,
    generateConfig,
    locale,
    onChange,
    divRef
  } = props;
  const month = generateConfig.getMonth(value || generateConfig.getNow());
  let start = 0;
  let end = 11;
  if (validRange) {
    const [rangeStart, rangeEnd] = validRange;
    const currentYear = generateConfig.getYear(value);
    if (generateConfig.getYear(rangeEnd) === currentYear) {
      end = generateConfig.getMonth(rangeEnd);
    }
    if (generateConfig.getYear(rangeStart) === currentYear) {
      start = generateConfig.getMonth(rangeStart);
    }
  }
  const months = locale.shortMonths || generateConfig.locale.getShortMonths(locale.locale);
  const options = [];
  for (let index = start; index <= end; index += 1) {
    options.push({
      label: months[index],
      value: index
    });
  }
  return /*#__PURE__*/React.createElement(_select.default, {
    size: fullscreen ? undefined : 'small',
    className: `${prefixCls}-month-select`,
    value: month,
    options: options,
    onChange: newMonth => {
      onChange(generateConfig.setMonth(value, newMonth));
    },
    getPopupContainer: () => divRef.current
  });
}
function ModeSwitch(props) {
  const {
    prefixCls,
    locale,
    mode,
    fullscreen,
    onModeChange
  } = props;
  return /*#__PURE__*/React.createElement(_radio.Group, {
    onChange: _ref => {
      let {
        target: {
          value
        }
      } = _ref;
      onModeChange(value);
    },
    value: mode,
    size: fullscreen ? undefined : 'small',
    className: `${prefixCls}-mode-switch`
  }, /*#__PURE__*/React.createElement(_radio.Button, {
    value: "month"
  }, locale.month), /*#__PURE__*/React.createElement(_radio.Button, {
    value: "year"
  }, locale.year));
}
function CalendarHeader(props) {
  const {
    prefixCls,
    fullscreen,
    mode,
    onChange,
    onModeChange
  } = props;
  const divRef = React.useRef(null);
  const formItemInputContext = (0, _react.useContext)(_context.FormItemInputContext);
  const mergedFormItemInputContext = (0, _react.useMemo)(() => Object.assign(Object.assign({}, formItemInputContext), {
    isFormItemInput: false
  }), [formItemInputContext]);
  const sharedProps = Object.assign(Object.assign({}, props), {
    fullscreen,
    divRef
  });
  return /*#__PURE__*/React.createElement("div", {
    className: `${prefixCls}-header`,
    ref: divRef
  }, /*#__PURE__*/React.createElement(_context.FormItemInputContext.Provider, {
    value: mergedFormItemInputContext
  }, /*#__PURE__*/React.createElement(YearSelect, Object.assign({}, sharedProps, {
    onChange: v => {
      onChange(v, 'year');
    }
  })), mode === 'month' && ( /*#__PURE__*/React.createElement(MonthSelect, Object.assign({}, sharedProps, {
    onChange: v => {
      onChange(v, 'month');
    }
  })))), /*#__PURE__*/React.createElement(ModeSwitch, Object.assign({}, sharedProps, {
    onModeChange: onModeChange
  })));
}
var _default = exports.default = CalendarHeader;