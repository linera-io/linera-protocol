"use strict";

var _interopRequireWildcard = require("@babel/runtime/helpers/interopRequireWildcard").default;
var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault").default;
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = useMutateObserver;
var _canUseDom = _interopRequireDefault(require("rc-util/lib/Dom/canUseDom"));
var React = _interopRequireWildcard(require("react"));
var defaultOptions = {
  subtree: true,
  childList: true,
  attributeFilter: ['style', 'class']
};
function useMutateObserver(nodeOrList, callback) {
  var options = arguments.length > 2 && arguments[2] !== undefined ? arguments[2] : defaultOptions;
  React.useEffect(function () {
    if (!(0, _canUseDom.default)() || !nodeOrList) {
      return;
    }
    var instance;
    var nodeList = Array.isArray(nodeOrList) ? nodeOrList : [nodeOrList];
    if ('MutationObserver' in window) {
      instance = new MutationObserver(callback);
      nodeList.forEach(function (element) {
        instance.observe(element, options);
      });
    }
    return function () {
      var _instance, _instance2;
      (_instance = instance) === null || _instance === void 0 ? void 0 : _instance.takeRecords();
      (_instance2 = instance) === null || _instance2 === void 0 ? void 0 : _instance2.disconnect();
    };
  }, [options, nodeOrList]);
}