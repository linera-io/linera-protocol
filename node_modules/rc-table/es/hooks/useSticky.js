import _typeof from "@babel/runtime/helpers/esm/typeof";
import * as React from 'react';
import canUseDom from "rc-util/es/Dom/canUseDom";
// fix ssr render
var defaultContainer = canUseDom() ? window : null;

/** Sticky header hooks */
export default function useSticky(sticky, prefixCls) {
  var _ref = _typeof(sticky) === 'object' ? sticky : {},
    _ref$offsetHeader = _ref.offsetHeader,
    offsetHeader = _ref$offsetHeader === void 0 ? 0 : _ref$offsetHeader,
    _ref$offsetSummary = _ref.offsetSummary,
    offsetSummary = _ref$offsetSummary === void 0 ? 0 : _ref$offsetSummary,
    _ref$offsetScroll = _ref.offsetScroll,
    offsetScroll = _ref$offsetScroll === void 0 ? 0 : _ref$offsetScroll,
    _ref$getContainer = _ref.getContainer,
    getContainer = _ref$getContainer === void 0 ? function () {
      return defaultContainer;
    } : _ref$getContainer;
  var container = getContainer() || defaultContainer;
  return React.useMemo(function () {
    var isSticky = !!sticky;
    return {
      isSticky: isSticky,
      stickyClassName: isSticky ? "".concat(prefixCls, "-sticky-holder") : '',
      offsetHeader: offsetHeader,
      offsetSummary: offsetSummary,
      offsetScroll: offsetScroll,
      container: container
    };
  }, [offsetScroll, offsetHeader, offsetSummary, prefixCls, container]);
}