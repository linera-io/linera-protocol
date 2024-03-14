import _toConsumableArray from "@babel/runtime/helpers/esm/toConsumableArray";
export function findValidateTime(date, getHourUnits, getMinuteUnits, getSecondUnits, getMillisecondUnits, generateConfig) {
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
      var reverseEnabledUnits = _toConsumableArray(validateUnits).reverse();
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