import useMemo from "rc-util/es/hooks/useMemo";
import isEqual from "rc-util/es/isEqual";
import { getCellFixedInfo } from "../utils/fixUtil";
export default function useFixedInfo(flattenColumns, stickyOffsets, direction) {
  var fixedInfoList = flattenColumns.map(function (_, colIndex) {
    return getCellFixedInfo(colIndex, colIndex, flattenColumns, stickyOffsets, direction);
  });
  return useMemo(function () {
    return fixedInfoList;
  }, [fixedInfoList], function (prev, next) {
    return !isEqual(prev, next);
  });
}