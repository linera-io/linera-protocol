"use strict";
"use client";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault").default;
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = void 0;
var _generateRangePicker = _interopRequireDefault(require("./generateRangePicker"));
var _generateSinglePicker = _interopRequireDefault(require("./generateSinglePicker"));
function generatePicker(generateConfig) {
  // =========================== Picker ===========================
  const {
    DatePicker,
    WeekPicker,
    MonthPicker,
    YearPicker,
    TimePicker,
    QuarterPicker
  } = (0, _generateSinglePicker.default)(generateConfig);
  // ======================== Range Picker ========================
  const RangePicker = (0, _generateRangePicker.default)(generateConfig);
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
var _default = exports.default = generatePicker;