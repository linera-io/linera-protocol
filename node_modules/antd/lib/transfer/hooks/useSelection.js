"use strict";

var _interopRequireWildcard = require("@babel/runtime/helpers/interopRequireWildcard").default;
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = useSelection;
var React = _interopRequireWildcard(require("react"));
const EMPTY_KEYS = [];
function filterKeys(keys, dataKeys) {
  const filteredKeys = keys.filter(key => dataKeys.has(key));
  return keys.length === filteredKeys.length ? keys : filteredKeys;
}
function flattenKeys(keys) {
  return Array.from(keys).join(';');
}
function useSelection(leftDataSource, rightDataSource) {
  let selectedKeys = arguments.length > 2 && arguments[2] !== undefined ? arguments[2] : EMPTY_KEYS;
  // Prepare `dataSource` keys
  const [leftKeys, rightKeys] = React.useMemo(() => [new Set(leftDataSource.map(src => src.key)), new Set(rightDataSource.map(src => src.key))], [leftDataSource, rightDataSource]);
  // Selected Keys
  const [sourceSelectedKeys, setSourceSelectedKeys] = React.useState(() => filterKeys(selectedKeys, leftKeys));
  const [targetSelectedKeys, setTargetSelectedKeys] = React.useState(() => filterKeys(selectedKeys, rightKeys));
  // Fill selected keys
  React.useEffect(() => {
    setSourceSelectedKeys(filterKeys(selectedKeys, leftKeys));
    setTargetSelectedKeys(filterKeys(selectedKeys, rightKeys));
  }, [selectedKeys]);
  // Reset when data changed
  React.useEffect(() => {
    setSourceSelectedKeys(filterKeys(sourceSelectedKeys, leftKeys));
    setTargetSelectedKeys(filterKeys(targetSelectedKeys, rightKeys));
  }, [flattenKeys(leftKeys), flattenKeys(rightKeys)]);
  return [
  // Keys
  sourceSelectedKeys, targetSelectedKeys,
  // Updater
  setSourceSelectedKeys, setTargetSelectedKeys];
}