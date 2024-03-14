import _slicedToArray from "@babel/runtime/helpers/esm/slicedToArray";
import _typeof from "@babel/runtime/helpers/esm/typeof";
import useMemo from "rc-util/es/hooks/useMemo";
import isEqual from "rc-util/es/isEqual";
import getValue from "rc-util/es/utils/get";
import warning from "rc-util/es/warning";
import * as React from 'react';
import PerfContext from "../context/PerfContext";
import { validateValue } from "../utils/valueUtil";
import { useImmutableMark } from "../context/TableContext";
function isRenderCell(data) {
  return data && _typeof(data) === 'object' && !Array.isArray(data) && ! /*#__PURE__*/React.isValidElement(data);
}
export default function useCellRender(record, dataIndex, renderIndex, children, render, shouldCellUpdate) {
  // TODO: Remove this after next major version
  var perfRecord = React.useContext(PerfContext);
  var mark = useImmutableMark();

  // ======================== Render ========================
  var retData = useMemo(function () {
    if (validateValue(children)) {
      return [children];
    }
    var path = dataIndex === null || dataIndex === undefined || dataIndex === '' ? [] : Array.isArray(dataIndex) ? dataIndex : [dataIndex];
    var value = getValue(record, path);

    // Customize render node
    var returnChildNode = value;
    var returnCellProps = undefined;
    if (render) {
      var renderData = render(value, record, renderIndex);
      if (isRenderCell(renderData)) {
        if (process.env.NODE_ENV !== 'production') {
          warning(false, '`columns.render` return cell props is deprecated with perf issue, please use `onCell` instead.');
        }
        returnChildNode = renderData.children;
        returnCellProps = renderData.props;
        perfRecord.renderWithProps = true;
      } else {
        returnChildNode = renderData;
      }
    }
    return [returnChildNode, returnCellProps];
  }, [
  // Force update deps
  mark,
  // Normal deps
  record, children, dataIndex, render, renderIndex], function (prev, next) {
    if (shouldCellUpdate) {
      var _prev = _slicedToArray(prev, 2),
        prevRecord = _prev[1];
      var _next = _slicedToArray(next, 2),
        nextRecord = _next[1];
      return shouldCellUpdate(nextRecord, prevRecord);
    }

    // Legacy mode should always update
    if (perfRecord.renderWithProps) {
      return true;
    }
    return !isEqual(prev, next, true);
  });
  return retData;
}