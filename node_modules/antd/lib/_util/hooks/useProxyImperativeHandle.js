"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = useProxyImperativeHandle;
var _react = require("react");
// Proxy the dom ref with `{ nativeElement, otherFn }` type
// ref: https://github.com/ant-design/ant-design/discussions/45242

function fillProxy(element, handler) {
  element._antProxy = element._antProxy || {};
  Object.keys(handler).forEach(key => {
    if (!(key in element._antProxy)) {
      const ori = element[key];
      element._antProxy[key] = ori;
      element[key] = handler[key];
    }
  });
  return element;
}
function useProxyImperativeHandle(ref, init) {
  return (0, _react.useImperativeHandle)(ref, () => {
    const refObj = init();
    const {
      nativeElement
    } = refObj;
    if (typeof Proxy !== 'undefined') {
      return new Proxy(nativeElement, {
        get(obj, prop) {
          if (refObj[prop]) {
            return refObj[prop];
          }
          return Reflect.get(obj, prop);
        }
      });
    }
    // Fallback of IE
    return fillProxy(nativeElement, refObj);
  });
}