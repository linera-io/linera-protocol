"use strict";
"use client";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault").default;
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = void 0;
var _dayjs = _interopRequireDefault(require("rc-picker/lib/generate/dayjs"));
var _generateCalendar = _interopRequireDefault(require("./generateCalendar"));
const Calendar = (0, _generateCalendar.default)(_dayjs.default);
Calendar.generateCalendar = _generateCalendar.default;
var _default = exports.default = Calendar;