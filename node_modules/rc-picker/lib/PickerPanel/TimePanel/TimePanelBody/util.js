"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault");
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.findValidateTime = findValidateTime;
var _toConsumableArray2 = _interopRequireDefault(require("@babel/runtime/helpers/toConsumableArray"));
function findValidateTime(date, getHourUnits, getMinuteUnits, getSecondUnits, getMillisecondUnits, generateConfig) {
  var nextDate = date;
  function alignValidate(getUnitValue, setUnitValue, units) {
    var nextValue = generateConfig[getUnitValue](nextDate);
    var nextUnit = units.find(function (unit) {
      return unit.value === nextValue;
    });
    if (!nextUnit || nextUnit.disabled) {
      // Find most closest unit
      var validateUnits = units.filter(function (unit) {
        return !unit.disabled;
      });
      var reverseEnabledUnits = (0, _toConsumableArray2.default)(validateUnits).reverse();
      var validateUnit = reverseEnabledUnits.find(function (unit) {
        return unit.value <= nextValue;
      }) || validateUnits[0];
      if (validateUnit) {
        nextValue = validateUnit.value;
        nextDate = generateConfig[setUnitValue](nextDate, nextValue);
      }
    }
    return nextValue;
  }

  // Find validate hour
  var nextHour = alignValidate('getHour', 'setHour', getHourUnits());

  // Find validate minute
  var nextMinute = alignValidate('getMinute', 'setMinute', getMinuteUnits(nextHour));

  // Find validate second
  var nextSecond = alignValidate('getSecond', 'setSecond', getSecondUnits(nextHour, nextMinute));

  // Find validate millisecond
  alignValidate('getMillisecond', 'setMillisecond', getMillisecondUnits(nextHour, nextMinute, nextSecond));
  return nextDate;
}