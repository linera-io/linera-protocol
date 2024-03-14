import _objectSpread from "@babel/runtime/helpers/esm/objectSpread2";
import { warning } from 'rc-util';
import * as React from 'react';
export default function useCellRender(cellRender, dateRender, monthCellRender, range) {
  // ========================= Warn =========================
  if (process.env.NODE_ENV !== 'production') {
    warning(!dateRender, "'dateRender' is deprecated. Please use 'cellRender' instead.");
    warning(!monthCellRender, "'monthCellRender' is deprecated. Please use 'cellRender' instead.");
  }

  // ======================== Render ========================
  // Merged render
  var mergedCellRender = React.useMemo(function () {
    if (cellRender) {
      return cellRender;
    }
    return function (current, info) {
      var date = current;
      if (dateRender && info.type === 'date') {
        return dateRender(date, info.today);
      }
      if (monthCellRender && info.type === 'month') {
        return monthCellRender(date, info.locale);
      }
      return info.originNode;
    };
  }, [cellRender, monthCellRender, dateRender]);

  // Cell render
  var onInternalCellRender = React.useCallback(function (date, info) {
    return mergedCellRender(date, _objectSpread(_objectSpread({}, info), {}, {
      range: range
    }));
  }, [mergedCellRender, range]);
  return onInternalCellRender;
}