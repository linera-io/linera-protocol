/* istanbul ignore file */
import * as React from 'react';
function useRenderTimes(props, debug) {
  // Render times
  var timesRef = React.useRef(0);
  timesRef.current += 1;

  // Props changed
  var propsRef = React.useRef(props);
  var keys = [];
  Object.keys(props || {}).map(function (key) {
    var _propsRef$current;
    if ((props === null || props === void 0 ? void 0 : props[key]) !== ((_propsRef$current = propsRef.current) === null || _propsRef$current === void 0 ? void 0 : _propsRef$current[key])) {
      keys.push(key);
    }
  });
  propsRef.current = props;

  // Cache keys since React rerender may cause it lost
  var keysRef = React.useRef([]);
  if (keys.length) {
    keysRef.current = keys;
  }
  React.useDebugValue(timesRef.current);
  React.useDebugValue(keysRef.current.join(', '));
  if (debug) {
    console.log("".concat(debug, ":"), timesRef.current, keysRef.current);
  }
  return timesRef.current;
}
export default process.env.NODE_ENV !== 'production' ? useRenderTimes : function () {};
export var RenderBlock = /*#__PURE__*/React.memo(function () {
  var times = useRenderTimes();
  return /*#__PURE__*/React.createElement("h1", null, "Render Times: ", times);
});
if (process.env.NODE_ENV !== 'production') {
  RenderBlock.displayName = 'RenderBlock';
}