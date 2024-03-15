import _defineProperty from "@babel/runtime/helpers/esm/defineProperty";
import _toConsumableArray from "@babel/runtime/helpers/esm/toConsumableArray";
import _slicedToArray from "@babel/runtime/helpers/esm/slicedToArray";
import classNames from 'classnames';
import useLayoutEffect from "rc-util/es/hooks/useLayoutEffect";
import * as React from 'react';
import { usePanelContext } from "../../context";
import useScrollTo from "./useScrollTo";
var SCROLL_DELAY = 300;
export default function TimeColumn(props) {
  var units = props.units,
    value = props.value,
    optionalValue = props.optionalValue,
    type = props.type,
    onChange = props.onChange,
    onDblClick = props.onDblClick,
    changeOnScroll = props.changeOnScroll;
  var _usePanelContext = usePanelContext(),
    prefixCls = _usePanelContext.prefixCls,
    cellRender = _usePanelContext.cellRender,
    now = _usePanelContext.now,
    locale = _usePanelContext.locale;
  var panelPrefixCls = "".concat(prefixCls, "-time-panel");
  var cellPrefixCls = "".concat(prefixCls, "-time-panel-cell");

  // ========================== Refs ==========================
  var ulRef = React.useRef(null);

  // ========================= Scroll =========================
  var checkDelayRef = React.useRef();
  var clearDelayCheck = function clearDelayCheck() {
    clearTimeout(checkDelayRef.current);
  };

  // ========================== Sync ==========================
  var _useScrollTo = useScrollTo(ulRef, value !== null && value !== void 0 ? value : optionalValue),
    _useScrollTo2 = _slicedToArray(_useScrollTo, 3),
    syncScroll = _useScrollTo2[0],
    stopScroll = _useScrollTo2[1],
    isScrolling = _useScrollTo2[2];

  // Effect sync value scroll
  useLayoutEffect(function () {
    syncScroll();
    clearDelayCheck();
    return function () {
      stopScroll();
      clearDelayCheck();
    };
  }, [value, optionalValue, units]);

  // ========================= Change =========================
  // Scroll event if sync onScroll
  var onInternalScroll = function onInternalScroll(event) {
    clearDelayCheck();
    var target = event.target;
    if (!isScrolling() && changeOnScroll) {
      checkDelayRef.current = setTimeout(function () {
        var ul = ulRef.current;
        var firstLiTop = ul.querySelector("li").offsetTop;
        var liList = Array.from(ul.querySelectorAll("li"));
        var liTopList = liList.map(function (li) {
          return li.offsetTop - firstLiTop;
        });
        var liDistList = liTopList.map(function (top, index) {
          if (units[index].disabled) {
            return Number.MAX_SAFE_INTEGER;
          }
          return Math.abs(top - target.scrollTop);
        });

        // Find min distance index
        var minDist = Math.min.apply(Math, _toConsumableArray(liDistList));
        var minDistIndex = liDistList.findIndex(function (dist) {
          return dist === minDist;
        });
        var targetUnit = units[minDistIndex];
        if (targetUnit && !targetUnit.disabled) {
          onChange(targetUnit.value);
        }
      }, SCROLL_DELAY);
    }
  };

  // ========================= Render =========================
  var columnPrefixCls = "".concat(panelPrefixCls, "-column");
  return /*#__PURE__*/React.createElement("ul", {
    className: columnPrefixCls,
    ref: ulRef,
    "data-type": type,
    onScroll: onInternalScroll
  }, units.map(function (_ref) {
    var label = _ref.label,
      unitValue = _ref.value,
      disabled = _ref.disabled;
    var inner = /*#__PURE__*/React.createElement("div", {
      className: "".concat(cellPrefixCls, "-inner")
    }, label);
    return /*#__PURE__*/React.createElement("li", {
      key: unitValue,
      className: classNames(cellPrefixCls, _defineProperty(_defineProperty({}, "".concat(cellPrefixCls, "-selected"), value === unitValue), "".concat(cellPrefixCls, "-disabled"), disabled)),
      onClick: function onClick() {
        if (!disabled) {
          onChange(unitValue);
        }
      },
      onDoubleClick: function onDoubleClick() {
        if (!disabled && onDblClick) {
          onDblClick();
        }
      },
      "data-value": unitValue
    }, cellRender ? cellRender(unitValue, {
      prefixCls: prefixCls,
      originNode: inner,
      today: now,
      type: 'time',
      subType: type,
      locale: locale
    }) : inner);
  }));
}