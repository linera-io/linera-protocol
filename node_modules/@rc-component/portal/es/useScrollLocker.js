import _slicedToArray from "@babel/runtime/helpers/esm/slicedToArray";
import * as React from 'react';
import { updateCSS, removeCSS } from "rc-util/es/Dom/dynamicCSS";
import useLayoutEffect from "rc-util/es/hooks/useLayoutEffect";
import { getTargetScrollBarSize } from "rc-util/es/getScrollBarSize";
import { isBodyOverflowing } from "./util";
var UNIQUE_ID = "rc-util-locker-".concat(Date.now());
var uuid = 0;
export default function useScrollLocker(lock) {
  var mergedLock = !!lock;
  var _React$useState = React.useState(function () {
      uuid += 1;
      return "".concat(UNIQUE_ID, "_").concat(uuid);
    }),
    _React$useState2 = _slicedToArray(_React$useState, 1),
    id = _React$useState2[0];
  useLayoutEffect(function () {
    if (mergedLock) {
      var scrollbarSize = getTargetScrollBarSize(document.body).width;
      var isOverflow = isBodyOverflowing();
      updateCSS("\nhtml body {\n  overflow-y: hidden;\n  ".concat(isOverflow ? "width: calc(100% - ".concat(scrollbarSize, "px);") : '', "\n}"), id);
    } else {
      removeCSS(id);
    }
    return function () {
      removeCSS(id);
    };
  }, [mergedLock, id]);
}