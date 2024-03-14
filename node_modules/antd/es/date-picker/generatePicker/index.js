"use client";

import generateRangePicker from './generateRangePicker';
import generateSinglePicker from './generateSinglePicker';
function generatePicker(generateConfig) {
  // =========================== Picker ===========================
  const {
    DatePicker,
    WeekPicker,
    MonthPicker,
    YearPicker,
    TimePicker,
    QuarterPicker
  } = generateSinglePicker(generateConfig);
  // ======================== Range Picker ========================
  const RangePicker = generateRangePicker(generateConfig);
  const MergedDatePicker = DatePicker;
  MergedDatePicker.WeekPicker = WeekPicker;
  MergedDatePicker.MonthPicker = MonthPicker;
  MergedDatePicker.YearPicker = YearPicker;
  MergedDatePicker.RangePicker = RangePicker;
  MergedDatePicker.TimePicker = TimePicker;
  MergedDatePicker.QuarterPicker = QuarterPicker;
  if (process.env.NODE_ENV !== 'production') {
    MergedDatePicker.displayName = 'DatePicker';
  }
  return MergedDatePicker;
}
export default generatePicker;