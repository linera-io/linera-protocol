"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault");
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = useFixedInfo;
var _useMemo = _interopRequireDefault(require("rc-util/lib/hooks/useMemo"));
var _isEqual = _interopRequireDefault(require("rc-util/lib/isEqual"));
var _fixUtil = require("../utils/fixUtil");
function useFixedInfo(flattenColumns, stickyOffsets, direction) {
  var fixedInfoList = flattenColumns.map(function (_, colIndex) {
    return (0, _fixUtil.getCellFixedInfo)(colIndex, colIndex, flattenColumns, stickyOffsets, direction);
  });
  return (0, _useMemo.default)(function () {
    return fixedInfoList;
  }, [fixedInfoList], function (prev, next) {
    return !(0, _isEqual.default)(prev, next);
  });
}