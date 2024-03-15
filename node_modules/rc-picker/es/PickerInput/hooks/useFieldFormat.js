import _typeof from "@babel/runtime/helpers/esm/typeof";
import * as React from 'react';
import { getRowFormat, toArray } from "../../utils/miscUtil";
export function useFieldFormat(picker, locale, format) {
  return React.useMemo(function () {
    var rawFormat = getRowFormat(picker, locale, format);
    var formatList = toArray(rawFormat);
    var firstFormat = formatList[0];
    var maskFormat = _typeof(firstFormat) === 'object' && firstFormat.type === 'mask' ? firstFormat.format : null;
    return [
    // Format list
    formatList.map(function (config) {
      return typeof config === 'string' || typeof config === 'function' ? config : config.format;
    }),
    // Mask Format
    maskFormat];
  }, [picker, locale, format]);
}