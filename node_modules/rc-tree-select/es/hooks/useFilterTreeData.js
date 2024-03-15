import _defineProperty from "@babel/runtime/helpers/esm/defineProperty";
import _objectSpread from "@babel/runtime/helpers/esm/objectSpread2";
import * as React from 'react';
import { fillLegacyProps } from "../utils/legacyUtil";
export default (function (treeData, searchValue, _ref) {
  var treeNodeFilterProp = _ref.treeNodeFilterProp,
    filterTreeNode = _ref.filterTreeNode,
    fieldNames = _ref.fieldNames;
  var fieldChildren = fieldNames.children;
  return React.useMemo(function () {
    if (!searchValue || filterTreeNode === false) {
      return treeData;
    }
    var filterOptionFunc;
    if (typeof filterTreeNode === 'function') {
      filterOptionFunc = filterTreeNode;
    } else {
      var upperStr = searchValue.toUpperCase();
      filterOptionFunc = function filterOptionFunc(_, dataNode) {
        var value = dataNode[treeNodeFilterProp];
        return String(value).toUpperCase().includes(upperStr);
      };
    }
    function dig(list) {
      var keepAll = arguments.length > 1 && arguments[1] !== undefined ? arguments[1] : false;
      return list.reduce(function (total, dataNode) {
        var children = dataNode[fieldChildren];
        var match = keepAll || filterOptionFunc(searchValue, fillLegacyProps(dataNode));
        var childList = dig(children || [], match);
        if (match || childList.length) {
          total.push(_objectSpread(_objectSpread({}, dataNode), {}, _defineProperty({
            isLeaf: undefined
          }, fieldChildren, childList)));
        }
        return total;
      }, []);
    }
    return dig(treeData);
  }, [treeData, searchValue, fieldChildren, treeNodeFilterProp, filterTreeNode]);
});