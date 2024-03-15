"use client";

import * as React from 'react';
import classNames from 'classnames';
import { PickerPanel as RCPickerPanel } from 'rc-picker';
import useMergedState from "rc-util/es/hooks/useMergedState";
import { devUseWarning } from '../_util/warning';
import { ConfigContext } from '../config-provider';
import { useLocale } from '../locale';
import CalendarHeader from './Header';
import enUS from './locale/en_US';
import useStyle from './style';
function generateCalendar(generateConfig) {
  function isSameYear(date1, date2) {
    return date1 && date2 && generateConfig.getYear(date1) === generateConfig.getYear(date2);
  }
  function isSameMonth(date1, date2) {
    return isSameYear(date1, date2) && generateConfig.getMonth(date1) === generateConfig.getMonth(date2);
  }
  function isSameDate(date1, date2) {
    return isSameMonth(date1, date2) && generateConfig.getDate(date1) === generateConfig.getDate(date2);
  }
  const Calendar = props => {
    const {
      prefixCls: customizePrefixCls,
      className,
      rootClassName,
      style,
      dateFullCellRender,
      dateCellRender,
      monthFullCellRender,
      monthCellRender,
      cellRender,
      fullCellRender,
      headerRender,
      value,
      defaultValue,
      disabledDate,
      mode,
      validRange,
      fullscreen = true,
      onChange,
      onPanelChange,
      onSelect
    } = props;
    const {
      getPrefixCls,
      direction,
      calendar
    } = React.useContext(ConfigContext);
    const prefixCls = getPrefixCls('picker', customizePrefixCls);
    const calendarPrefixCls = `${prefixCls}-calendar`;
    const [wrapCSSVar, hashId, cssVarCls] = useStyle(prefixCls, calendarPrefixCls);
    const today = generateConfig.getNow();
    // ====================== Warning =======================
    if (process.env.NODE_ENV !== 'production') {
      const warning = devUseWarning('Calendar');
      warning.deprecated(!dateFullCellRender, 'dateFullCellRender', 'fullCellRender');
      warning.deprecated(!dateCellRender, 'dateCellRender', 'cellRender');
      warning.deprecated(!monthFullCellRender, 'monthFullCellRender', 'fullCellRender');
      warning.deprecated(!monthCellRender, 'monthCellRender', 'cellRender');
    }
    // ====================== State =======================
    // Value
    const [mergedValue, setMergedValue] = useMergedState(() => value || generateConfig.getNow(), {
      defaultValue,
      value
    });
    // Mode
    const [mergedMode, setMergedMode] = useMergedState('month', {
      value: mode
    });
    const panelMode = React.useMemo(() => mergedMode === 'year' ? 'month' : 'date', [mergedMode]);
    // Disabled Date
    const mergedDisabledDate = React.useCallback(date => {
      const notInRange = validRange ? generateConfig.isAfter(validRange[0], date) || generateConfig.isAfter(date, validRange[1]) : false;
      return notInRange || !!(disabledDate === null || disabledDate === void 0 ? void 0 : disabledDate(date));
    }, [disabledDate, validRange]);
    // ====================== Events ======================
    const triggerPanelChange = (date, newMode) => {
      onPanelChange === null || onPanelChange === void 0 ? void 0 : onPanelChange(date, newMode);
    };
    const triggerChange = date => {
      setMergedValue(date);
      if (!isSameDate(date, mergedValue)) {
        // Trigger when month panel switch month
        if (panelMode === 'date' && !isSameMonth(date, mergedValue) || panelMode === 'month' && !isSameYear(date, mergedValue)) {
          triggerPanelChange(date, mergedMode);
        }
        onChange === null || onChange === void 0 ? void 0 : onChange(date);
      }
    };
    const triggerModeChange = newMode => {
      setMergedMode(newMode);
      triggerPanelChange(mergedValue, newMode);
    };
    const onInternalSelect = (date, source) => {
      triggerChange(date);
      onSelect === null || onSelect === void 0 ? void 0 : onSelect(date, {
        source
      });
    };
    // ====================== Locale ======================
    const getDefaultLocale = () => {
      const {
        locale
      } = props;
      const result = Object.assign(Object.assign({}, enUS), locale);
      result.lang = Object.assign(Object.assign({}, result.lang), (locale || {}).lang);
      return result;
    };
    // ====================== Render ======================
    const dateRender = React.useCallback((date, info) => {
      if (fullCellRender) {
        return fullCellRender(date, info);
      }
      if (dateFullCellRender) {
        return dateFullCellRender(date);
      }
      return /*#__PURE__*/React.createElement("div", {
        className: classNames(`${prefixCls}-cell-inner`, `${calendarPrefixCls}-date`, {
          [`${calendarPrefixCls}-date-today`]: isSameDate(today, date)
        })
      }, /*#__PURE__*/React.createElement("div", {
        className: `${calendarPrefixCls}-date-value`
      }, String(generateConfig.getDate(date)).padStart(2, '0')), /*#__PURE__*/React.createElement("div", {
        className: `${calendarPrefixCls}-date-content`
      }, cellRender ? cellRender(date, info) : dateCellRender && dateCellRender(date)));
    }, [dateFullCellRender, dateCellRender, cellRender, fullCellRender]);
    const monthRender = React.useCallback((date, info) => {
      if (fullCellRender) {
        return fullCellRender(date, info);
      }
      if (monthFullCellRender) {
        return monthFullCellRender(date);
      }
      const months = info.locale.shortMonths || generateConfig.locale.getShortMonths(info.locale.locale);
      return /*#__PURE__*/React.createElement("div", {
        className: classNames(`${prefixCls}-cell-inner`, `${calendarPrefixCls}-date`, {
          [`${calendarPrefixCls}-date-today`]: isSameMonth(today, date)
        })
      }, /*#__PURE__*/React.createElement("div", {
        className: `${calendarPrefixCls}-date-value`
      }, months[generateConfig.getMonth(date)]), /*#__PURE__*/React.createElement("div", {
        className: `${calendarPrefixCls}-date-content`
      }, cellRender ? cellRender(date, info) : monthCellRender && monthCellRender(date)));
    }, [monthFullCellRender, monthCellRender, cellRender, fullCellRender]);
    const [contextLocale] = useLocale('Calendar', getDefaultLocale);
    const mergedCellRender = (current, info) => {
      if (info.type === 'date') {
        return dateRender(current, info);
      }
      if (info.type === 'month') {
        return monthRender(current, Object.assign(Object.assign({}, info), {
          locale: contextLocale === null || contextLocale === void 0 ? void 0 : contextLocale.lang
        }));
      }
    };
    return wrapCSSVar( /*#__PURE__*/React.createElement("div", {
      className: classNames(calendarPrefixCls, {
        [`${calendarPrefixCls}-full`]: fullscreen,
        [`${calendarPrefixCls}-mini`]: !fullscreen,
        [`${calendarPrefixCls}-rtl`]: direction === 'rtl'
      }, calendar === null || calendar === void 0 ? void 0 : calendar.className, className, rootClassName, hashId, cssVarCls),
      style: Object.assign(Object.assign({}, calendar === null || calendar === void 0 ? void 0 : calendar.style), style)
    }, headerRender ? headerRender({
      value: mergedValue,
      type: mergedMode,
      onChange: nextDate => {
        onInternalSelect(nextDate, 'customize');
      },
      onTypeChange: triggerModeChange
    }) : ( /*#__PURE__*/React.createElement(CalendarHeader, {
      prefixCls: calendarPrefixCls,
      value: mergedValue,
      generateConfig: generateConfig,
      mode: mergedMode,
      fullscreen: fullscreen,
      locale: contextLocale === null || contextLocale === void 0 ? void 0 : contextLocale.lang,
      validRange: validRange,
      onChange: onInternalSelect,
      onModeChange: triggerModeChange
    })), /*#__PURE__*/React.createElement(RCPickerPanel, {
      value: mergedValue,
      prefixCls: prefixCls,
      locale: contextLocale === null || contextLocale === void 0 ? void 0 : contextLocale.lang,
      generateConfig: generateConfig,
      cellRender: mergedCellRender,
      onSelect: nextDate => {
        onInternalSelect(nextDate, panelMode);
      },
      mode: panelMode,
      picker: panelMode,
      disabledDate: mergedDisabledDate,
      hideHeader: true
    })));
  };
  if (process.env.NODE_ENV !== 'production') {
    Calendar.displayName = 'Calendar';
  }
  return Calendar;
}
export default generateCalendar;