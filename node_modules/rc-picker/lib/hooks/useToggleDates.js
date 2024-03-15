"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault");
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = useToggleDates;
var _toConsumableArray2 = _interopRequireDefault(require("@babel/runtime/helpers/toConsumableArray"));
var _dateUtil = require("../utils/dateUtil");
/**
 * Toggles the presence of a value in an array.
 * If the value exists in the array, removed it.
 * Else add it.
 */
function useToggleDates(generateConfig, locale, panelMode) {
  function toggleDates(list, target) {
    var index = list.findIndex(function (date) {
      return (0, _dateUtil.isSame)(generateConfig, locale, date, target, panelMode);
    });
    if (index === -1) {
      return [].concat((0, _toConsumableArray2.default)(list), [target]);
    }
    var sliceList = (0, _toConsumableArray2.default)(list);
    sliceList.splice(index, 1);
    return sliceList;
  }
  return toggleDates;
}