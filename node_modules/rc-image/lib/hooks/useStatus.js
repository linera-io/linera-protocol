"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault");
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = useStatus;
var _slicedToArray2 = _interopRequireDefault(require("@babel/runtime/helpers/slicedToArray"));
var _react = require("react");
var _util = require("../util");
function useStatus(_ref) {
  var src = _ref.src,
    isCustomPlaceholder = _ref.isCustomPlaceholder,
    fallback = _ref.fallback;
  var _useState = (0, _react.useState)(isCustomPlaceholder ? 'loading' : 'normal'),
    _useState2 = (0, _slicedToArray2.default)(_useState, 2),
    status = _useState2[0],
    setStatus = _useState2[1];
  var isLoaded = (0, _react.useRef)(false);
  var isError = status === 'error';

  // https://github.com/react-component/image/pull/187
  (0, _react.useEffect)(function () {
    var isCurrentSrc = true;
    (0, _util.isImageValid)(src).then(function (isValid) {
      // https://github.com/ant-design/ant-design/issues/44948
      // If src changes, the previous setStatus should not be triggered
      if (!isValid && isCurrentSrc) {
        setStatus('error');
      }
    });
    return function () {
      isCurrentSrc = false;
    };
  }, [src]);
  (0, _react.useEffect)(function () {
    if (isCustomPlaceholder && !isLoaded.current) {
      setStatus('loading');
    } else if (isError) {
      setStatus('normal');
    }
  }, [src]);
  var onLoad = function onLoad() {
    setStatus('normal');
  };
  var getImgRef = function getImgRef(img) {
    isLoaded.current = false;
    if (status === 'loading' && img !== null && img !== void 0 && img.complete && (img.naturalWidth || img.naturalHeight)) {
      isLoaded.current = true;
      onLoad();
    }
  };
  var srcAndOnload = isError && fallback ? {
    src: fallback
  } : {
    onLoad: onLoad,
    src: src
  };
  return [getImgRef, srcAndOnload, status];
}