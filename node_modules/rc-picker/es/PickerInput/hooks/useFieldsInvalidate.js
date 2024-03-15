import _slicedToArray from "@babel/runtime/helpers/esm/slicedToArray";
import { fillIndex } from "../../utils/miscUtil";
import * as React from 'react';
/**
 * Used to control each fields invalidate status
 */
export default function useFieldsInvalidate(calendarValue, isInvalidateDate) {
  var allowEmpty = arguments.length > 2 && arguments[2] !== undefined ? arguments[2] : [];
  var _React$useState = React.useState([false, false]),
    _React$useState2 = _slicedToArray(_React$useState, 2),
    fieldsInvalidates = _React$useState2[0],
    setFieldsInvalidates = _React$useState2[1];
  var onSelectorInvalid = function onSelectorInvalid(invalid, index) {
    setFieldsInvalidates(function (ori) {
      return fillIndex(ori, index, invalid);
    });
  };

  /**
   * For the Selector Input to mark as `aria-disabled`
   */
  var submitInvalidates = React.useMemo(function () {
    return fieldsInvalidates.map(function (invalid, index) {
      // If typing invalidate
      if (invalid) {
        return true;
      }
      var current = calendarValue[index];

      // Not check if all empty
      if (!current) {
        return false;
      }

      // Not allow empty
      if (!allowEmpty[index] && !current) {
        return true;
      }

      // Invalidate
      if (current && isInvalidateDate(current, {
        activeIndex: index
      })) {
        return true;
      }
      return false;
    });
  }, [calendarValue, fieldsInvalidates, isInvalidateDate, allowEmpty]);
  return [submitInvalidates, onSelectorInvalid];
}