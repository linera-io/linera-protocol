import _extends from "@babel/runtime/helpers/esm/extends";
import _objectSpread from "@babel/runtime/helpers/esm/objectSpread2";
import * as React from 'react';
import PickerPanel from "../../PickerPanel";
import { PickerHackContext } from "../../PickerPanel/context";
import PickerContext from "../context";
import { offsetPanelDate } from "../hooks/useRangePickerValue";
export default function PopupPanel(props) {
  var picker = props.picker,
    multiplePanel = props.multiplePanel,
    pickerValue = props.pickerValue,
    onPickerValueChange = props.onPickerValueChange,
    needConfirm = props.needConfirm,
    onSubmit = props.onSubmit,
    range = props.range,
    hoverValue = props.hoverValue;
  var _React$useContext = React.useContext(PickerContext),
    prefixCls = _React$useContext.prefixCls,
    generateConfig = _React$useContext.generateConfig;

  // ======================== Offset ========================
  var internalOffsetDate = React.useCallback(function (date, offset) {
    return offsetPanelDate(generateConfig, picker, date, offset);
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
  var pickerProps = _objectSpread(_objectSpread({}, props), {}, {
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
    }, /*#__PURE__*/React.createElement(PickerHackContext.Provider, {
      value: _objectSpread(_objectSpread({}, sharedContext), {}, {
        hideNext: true
      })
    }, /*#__PURE__*/React.createElement(PickerPanel, pickerProps)), /*#__PURE__*/React.createElement(PickerHackContext.Provider, {
      value: _objectSpread(_objectSpread({}, sharedContext), {}, {
        hidePrev: true
      })
    }, /*#__PURE__*/React.createElement(PickerPanel, _extends({}, pickerProps, {
      pickerValue: nextPickerValue,
      onPickerValueChange: onSecondPickerValueChange
    }))));
  }

  // Single
  return /*#__PURE__*/React.createElement(PickerHackContext.Provider, {
    value: _objectSpread({}, sharedContext)
  }, /*#__PURE__*/React.createElement(PickerPanel, pickerProps));
}