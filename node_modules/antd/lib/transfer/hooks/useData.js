"use strict";

var _interopRequireWildcard = require("@babel/runtime/helpers/interopRequireWildcard").default;
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = void 0;
var React = _interopRequireWildcard(require("react"));
var _transKeys = require("../../_util/transKeys");
const useData = (dataSource, rowKey, targetKeys) => {
  const mergedDataSource = React.useMemo(() => (dataSource || []).map(record => {
    if (rowKey) {
      record = Object.assign(Object.assign({}, record), {
        key: rowKey(record)
      });
    }
    return record;
  }), [dataSource, rowKey]);
  const [leftDataSource, rightDataSource] = React.useMemo(() => {
    const leftData = [];
    const rightData = new Array((targetKeys || []).length);
    const targetKeysMap = (0, _transKeys.groupKeysMap)(targetKeys || []);
    mergedDataSource.forEach(record => {
      // rightData should be ordered by targetKeys
      // leftData should be ordered by dataSource
      if (targetKeysMap.has(record.key)) {
        rightData[targetKeysMap.get(record.key)] = record;
      } else {
        leftData.push(record);
      }
    });
    return [leftData, rightData];
  }, [mergedDataSource, targetKeys, rowKey]);
  return [mergedDataSource, leftDataSource, rightDataSource];
};
var _default = exports.default = useData;