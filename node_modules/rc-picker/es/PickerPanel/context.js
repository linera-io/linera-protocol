import * as React from 'react';
/** Used for each single Panel. e.g. DatePanel */
export var PanelContext = /*#__PURE__*/React.createContext(null);
export function usePanelContext() {
  return React.useContext(PanelContext);
}

/**
 * Get shared props for the SharedPanelProps interface.
 */
export function useInfo(props, panelType) {
  var prefixCls = props.prefixCls,
    generateConfig = props.generateConfig,
    locale = props.locale,
    disabledDate = props.disabledDate,
    minDate = props.minDate,
    maxDate = props.maxDate,
    cellRender = props.cellRender,
    hoverValue = props.hoverValue,
    hoverRangeValue = props.hoverRangeValue,
    onHover = props.onHover,
    values = props.values,
    pickerValue = props.pickerValue,
    onSelect = props.onSelect,
    prevIcon = props.prevIcon,
    nextIcon = props.nextIcon,
    superPrevIcon = props.superPrevIcon,
    superNextIcon = props.superNextIcon;

  // ========================= MISC =========================
  var now = generateConfig.getNow();

  // ========================= Info =========================
  var info = {
    now: now,
    values: values,
    pickerValue: pickerValue,
    prefixCls: prefixCls,
    disabledDate: disabledDate,
    minDate: minDate,
    maxDate: maxDate,
    cellRender: cellRender,
    hoverValue: hoverValue,
    hoverRangeValue: hoverRangeValue,
    onHover: onHover,
    locale: locale,
    generateConfig: generateConfig,
    onSelect: onSelect,
    panelType: panelType,
    // Icons
    prevIcon: prevIcon,
    nextIcon: nextIcon,
    superPrevIcon: superPrevIcon,
    superNextIcon: superNextIcon
  };
  return [info, now];
}

// ============================== Internal ==============================

/**
 * Internal usage for RangePicker to not to show the operation arrow
 */
export var PickerHackContext = /*#__PURE__*/React.createContext({});
if (process.env.NODE_ENV !== 'production') {
  PickerHackContext.displayName = 'PickerHackContext';
}