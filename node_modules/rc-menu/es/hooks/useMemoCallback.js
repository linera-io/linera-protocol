import * as React from 'react';

/**
 * Cache callback function that always return same ref instead.
 * This is used for context optimization.
 */
export default function useMemoCallback(func) {
  var funRef = React.useRef(func);
  funRef.current = func;
  var callback = React.useCallback(function () {
    var _funRef$current;
    for (var _len = arguments.length, args = new Array(_len), _key = 0; _key < _len; _key++) {
      args[_key] = arguments[_key];
    }
    return (_funRef$current = funRef.current) === null || _funRef$current === void 0 ? void 0 : _funRef$current.call.apply(_funRef$current, [funRef].concat(args));
  }, []);
  return func ? callback : undefined;
}