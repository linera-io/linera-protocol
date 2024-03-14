import _objectSpread from "@babel/runtime/helpers/esm/objectSpread2";
import * as React from 'react';
import { convertDataToEntities } from "rc-tree/es/utils/treeUtil";
import { VALUE_SPLIT } from "../utils/commonUtil";
/** Lazy parse options data into conduct-able info to avoid perf issue in single mode */
export default (function (options, fieldNames) {
  var cacheRef = React.useRef({
    options: null,
    info: null
  });
  var getEntities = React.useCallback(function () {
    if (cacheRef.current.options !== options) {
      cacheRef.current.options = options;
      cacheRef.current.info = convertDataToEntities(options, {
        fieldNames: fieldNames,
        initWrapper: function initWrapper(wrapper) {
          return _objectSpread(_objectSpread({}, wrapper), {}, {
            pathKeyEntities: {}
          });
        },
        processEntity: function processEntity(entity, wrapper) {
          var pathKey = entity.nodes.map(function (node) {
            return node[fieldNames.value];
          }).join(VALUE_SPLIT);
          wrapper.pathKeyEntities[pathKey] = entity;

          // Overwrite origin key.
          // this is very hack but we need let conduct logic work with connect path
          entity.key = pathKey;
        }
      });
    }
    return cacheRef.current.info.pathKeyEntities;
  }, [fieldNames, options]);
  return getEntities;
});