import { useContext } from '@rc-component/context';
import * as React from 'react';
import TableContext, { responseImmutable } from "../context/TableContext";
import devRenderTimes from "../hooks/useRenderTimes";
import Summary from "./Summary";
import SummaryContext from "./SummaryContext";
function Footer(props) {
  if (process.env.NODE_ENV !== 'production') {
    devRenderTimes(props);
  }
  var children = props.children,
    stickyOffsets = props.stickyOffsets,
    flattenColumns = props.flattenColumns;
  var prefixCls = useContext(TableContext, 'prefixCls');
  var lastColumnIndex = flattenColumns.length - 1;
  var scrollColumn = flattenColumns[lastColumnIndex];
  var summaryContext = React.useMemo(function () {
    return {
      stickyOffsets: stickyOffsets,
      flattenColumns: flattenColumns,
      scrollColumnIndex: scrollColumn !== null && scrollColumn !== void 0 && scrollColumn.scrollbar ? lastColumnIndex : null
    };
  }, [scrollColumn, flattenColumns, lastColumnIndex, stickyOffsets]);
  return /*#__PURE__*/React.createElement(SummaryContext.Provider, {
    value: summaryContext
  }, /*#__PURE__*/React.createElement("tfoot", {
    className: "".concat(prefixCls, "-summary")
  }, children));
}
export default responseImmutable(Footer);
export var FooterComponents = Summary;