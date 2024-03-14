import _objectSpread from "@babel/runtime/helpers/esm/objectSpread2";
import * as React from 'react';
import { RawList } from "./List";
var List = /*#__PURE__*/React.forwardRef(function (props, ref) {
  return RawList(_objectSpread(_objectSpread({}, props), {}, {
    virtual: false
  }), ref);
});
List.displayName = 'List';
export default List;