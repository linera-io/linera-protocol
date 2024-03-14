import _objectSpread from "@babel/runtime/helpers/esm/objectSpread2";
import * as React from 'react';
import { convertDataToEntities } from "rc-tree/es/utils/treeUtil";
import warning from "rc-util/es/warning";
import { isNil } from "../utils/valueUtil";
export default (function (treeData, fieldNames) {
  return React.useMemo(function () {
    var collection = convertDataToEntities(treeData, {
      fieldNames: fieldNames,
      initWrapper: function initWrapper(wrapper) {
        return _objectSpread(_objectSpread({}, wrapper), {}, {
          valueEntities: new Map()
        });
      },
      processEntity: function processEntity(entity, wrapper) {
        var val = entity.node[fieldNames.value];

        // Check if exist same value
        if (process.env.NODE_ENV !== 'production') {
          var key = entity.node.key;
          warning(!isNil(val), 'TreeNode `value` is invalidate: undefined');
          warning(!wrapper.valueEntities.has(val), "Same `value` exist in the tree: ".concat(val));
          warning(!key || String(key) === String(val), "`key` or `value` with TreeNode must be the same or you can remove one of them. key: ".concat(key, ", value: ").concat(val, "."));
        }
        wrapper.valueEntities.set(val, entity);
      }
    });
    return collection;
  }, [treeData, fieldNames]);
});