"use strict";
"use client";

var _interopRequireWildcard = require("@babel/runtime/helpers/interopRequireWildcard").default;
var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault").default;
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = SingleNumber;
var _classnames = _interopRequireDefault(require("classnames"));
var React = _interopRequireWildcard(require("react"));
function UnitNumber(_ref) {
  let {
    prefixCls,
    value,
    current,
    offset = 0
  } = _ref;
  let style;
  if (offset) {
    style = {
      position: 'absolute',
      top: `${offset}00%`,
      left: 0
    };
  }
  return /*#__PURE__*/React.createElement("span", {
    style: style,
    className: (0, _classnames.default)(`${prefixCls}-only-unit`, {
      current
    })
  }, value);
}
function getOffset(start, end, unit) {
  let index = start;
  let offset = 0;
  while ((index + 10) % 10 !== end) {
    index += unit;
    offset += unit;
  }
  return offset;
}
function SingleNumber(props) {
  const {
    prefixCls,
    count: originCount,
    value: originValue
  } = props;
  const value = Number(originValue);
  const count = Math.abs(originCount);
  const [prevValue, setPrevValue] = React.useState(value);
  const [prevCount, setPrevCount] = React.useState(count);
  // ============================= Events =============================
  const onTransitionEnd = () => {
    setPrevValue(value);
    setPrevCount(count);
  };
  // Fallback if transition events are not supported
  React.useEffect(() => {
    const timeout = setTimeout(() => {
      onTransitionEnd();
    }, 1000);
    return () => {
      clearTimeout(timeout);
    };
  }, [value]);
  // ============================= Render =============================
  // Render unit list
  let unitNodes;
  let offsetStyle;
  if (prevValue === value || Number.isNaN(value) || Number.isNaN(prevValue)) {
    // Nothing to change
    unitNodes = [/*#__PURE__*/React.createElement(UnitNumber, Object.assign({}, props, {
      key: value,
      current: true
    }))];
    offsetStyle = {
      transition: 'none'
    };
  } else {
    unitNodes = [];
    // Fill basic number units
    const end = value + 10;
    const unitNumberList = [];
    for (let index = value; index <= end; index += 1) {
      unitNumberList.push(index);
    }
    // Fill with number unit nodes
    const prevIndex = unitNumberList.findIndex(n => n % 10 === prevValue);
    unitNodes = unitNumberList.map((n, index) => {
      const singleUnit = n % 10;
      return /*#__PURE__*/React.createElement(UnitNumber, Object.assign({}, props, {
        key: n,
        value: singleUnit,
        offset: index - prevIndex,
        current: index === prevIndex
      }));
    });
    // Calculate container offset value
    const unit = prevCount < count ? 1 : -1;
    offsetStyle = {
      transform: `translateY(${-getOffset(prevValue, value, unit)}00%)`
    };
  }
  return /*#__PURE__*/React.createElement("span", {
    className: `${prefixCls}-only`,
    style: offsetStyle,
    onTransitionEnd: onTransitionEnd
  }, unitNodes);
}