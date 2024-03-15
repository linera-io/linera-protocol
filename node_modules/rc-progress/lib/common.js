"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.useTransitionDuration = exports.defaultProps = void 0;
var _react = require("react");
var defaultProps = {
  percent: 0,
  prefixCls: 'rc-progress',
  strokeColor: '#2db7f5',
  strokeLinecap: 'round',
  strokeWidth: 1,
  trailColor: '#D9D9D9',
  trailWidth: 1,
  gapPosition: 'bottom'
};
exports.defaultProps = defaultProps;
var useTransitionDuration = function useTransitionDuration() {
  var pathsRef = (0, _react.useRef)([]);
  var prevTimeStamp = (0, _react.useRef)(null);
  (0, _react.useEffect)(function () {
    var now = Date.now();
    var updated = false;
    pathsRef.current.forEach(function (path) {
      if (!path) {
        return;
      }
      updated = true;
      var pathStyle = path.style;
      pathStyle.transitionDuration = '.3s, .3s, .3s, .06s';
      if (prevTimeStamp.current && now - prevTimeStamp.current < 100) {
        pathStyle.transitionDuration = '0s, 0s';
      }
    });
    if (updated) {
      prevTimeStamp.current = Date.now();
    }
  });
  return pathsRef.current;
};
exports.useTransitionDuration = useTransitionDuration;