"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault");
var _typeof = require("@babel/runtime/helpers/typeof");
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = PopupPanel;
var _extends2 = _interopRequireDefault(require("@babel/runtime/helpers/extends"));
var _objectSpread2 = _interopRequireDefault(require("@babel/runtime/helpers/objectSpread2"));
var React = _interopRequireWildcard(require("react"));
var _PickerPanel = _interopRequireDefault(require("../../PickerPanel"));
var _context = require("../../PickerPanel/context");
var _context2 = _interopRequireDefault(require("../context"));
var _useRangePickerValue = require("../hooks/useRangePickerValue");
function _getRequireWildcardCache(e) { if ("function" != typeof WeakMap) return null; var r = new WeakMap(), t = new WeakMap(); return (_getRequireWildcardCache = function _getRequireWildcardCache(e) { return e ? t : r; })(e); }
function _interopRequireWildcard(e, r) { if (!r && e && e.__esModule) return e; if (null === e || "object" != _typeof(e) && "function" != typeof e) return { default: e }; var t = _getRequireWildcardCache(r); if (t && t.has(e)) return t.get(e); var n = { __proto__: null }, a = Object.defineProperty && Object.getOwnPropertyDescriptor; for (var u in e) if ("default" !== u && Object.prototype.hasOwnProperty.call(e, u)) { var i = a ? Object.getOwnPropertyDescriptor(e, u) : null; i && (i.get || i.set) ? Object.defineProperty(n, u, i) : n[u] = e[u]; } return n.default = e, t && t.set(e, n), n; }
function PopupPanel(props) {
  var picker = props.picker,
    multiplePanel = props.multiplePanel,
    pickerValue = props.pickerValue,
    onPickerValueChange = props.onPickerValueChange,
    needConfirm = props.needConfirm,
    onSubmit = props.onSubmit,
    range = props.range,
    hoverValue = props.hoverValue;
  var _React$useContext = React.useContext(_context2.default),
    prefixCls = _React$useContext.prefixCls,
    generateConfig = _React$useContext.generateConfig;

  // ======================== Offset ========================
  var internalOffsetDate = React.useCallback(function (date, offset) {
    return (0, _useRangePickerValue.offsetPanelDate)(generateConfig, picker, date, offset);
  }, [generateConfig, picker]);
  var nextPickerValue = React.useMemo(function () {
    return internalOffsetDate(pickerValue, 1);
  }, [pickerValue, internalOffsetDate]);

  // Outside
  var onSecondPickerValueChange = function onSecondPickerValueChange(nextDate) {
    onPickerValueChange(internalOffsetDate(nextDate, -1));
  };

  // ======================= Context ========================
  var sharedContext = {
    onCellDblClick: function onCellDblClick() {
      if (needConfirm) {
        onSubmit();
      }
    }
  };
  var hideHeader = picker === 'time';

  // ======================== Props =========================
  var pickerProps = (0, _objectSpread2.default)((0, _objectSpread2.default)({}, props), {}, {
    hoverValue: null,
    hoverRangeValue: null,
    hideHeader: hideHeader
  });
  if (range) {
    pickerProps.hoverRangeValue = hoverValue;
  } else {
    pickerProps.hoverValue = hoverValue;
  }

  // ======================== Render ========================
  // Multiple
  if (multiplePanel) {
    return /*#__PURE__*/React.createElement("div", {
      className: "".concat(prefixCls, "-panels")
    }, /*#__PURE__*/React.createElement(_context.PickerHackContext.Provider, {
      value: (0, _objectSpread2.default)((0, _objectSpread2.default)({}, sharedContext), {}, {
        hideNext: true
      })
    }, /*#__PURE__*/React.createElement(_PickerPanel.default, pickerProps)), /*#__PURE__*/React.createElement(_context.PickerHackContext.Provider, {
      value: (0, _objectSpread2.default)((0, _objectSpread2.default)({}, sharedContext), {}, {
        hidePrev: true
      })
    }, /*#__PURE__*/React.createElement(_PickerPanel.default, (0, _extends2.default)({}, pickerProps, {
      pickerValue: nextPickerValue,
      onPickerValueChange: onSecondPickerValueChange
    }))));
  }

  // Single
  return /*#__PURE__*/React.createElement(_context.PickerHackContext.Provider, {
    value: (0, _objectSpread2.default)({}, sharedContext)
  }, /*#__PURE__*/React.createElement(_PickerPanel.default, pickerProps));
}