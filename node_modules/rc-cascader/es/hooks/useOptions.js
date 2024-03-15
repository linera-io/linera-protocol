import * as React from 'react';
import useEntities from "./useEntities";
export default function useOptions(mergedFieldNames, options) {
  var mergedOptions = React.useMemo(function () {
    return options || [];
  }, [options]);

  // Only used in multiple mode, this fn will not call in single mode
  var getPathKeyEntities = useEntities(mergedOptions, mergedFieldNames);

  /** Convert path key back to value format */
  var getValueByKeyPath = React.useCallback(function (pathKeys) {
    var keyPathEntities = getPathKeyEntities();
    return pathKeys.map(function (pathKey) {
      var nodes = keyPathEntities[pathKey].nodes;
      return nodes.map(function (node) {
        return node[mergedFieldNames.value];
      });
    });
  }, [getPathKeyEntities, mergedFieldNames]);
  return [mergedOptions, getPathKeyEntities, getValueByKeyPath];
}