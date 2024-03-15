import _toConsumableArray from "@babel/runtime/helpers/esm/toConsumableArray";
import { isSame } from "../utils/dateUtil";
/**
 * Toggles the presence of a value in an array.
 * If the value exists in the array, removed it.
 * Else add it.
 */
export default function useToggleDates(generateConfig, locale, panelMode) {
  function toggleDates(list, target) {
    var index = list.findIndex(function (date) {
      return isSame(generateConfig, locale, date, target, panelMode);
    });
    if (index === -1) {
      return [].concat(_toConsumableArray(list), [target]);
    }
    var sliceList = _toConsumableArray(list);
    sliceList.splice(index, 1);
    return sliceList;
  }
  return toggleDates;
}