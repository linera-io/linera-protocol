import _extends from "@babel/runtime/helpers/esm/extends";
/* eslint-disable default-case */
import { useBaseProps } from 'rc-select';
import * as React from 'react';
import RawOptionList from "./List";
var RefOptionList = /*#__PURE__*/React.forwardRef(function (props, ref) {
  var baseProps = useBaseProps();

  // >>>>> Render
  return /*#__PURE__*/React.createElement(RawOptionList, _extends({}, props, baseProps, {
    ref: ref
  }));
});
export default RefOptionList;