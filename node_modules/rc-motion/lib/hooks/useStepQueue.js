"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault");
var _typeof = require("@babel/runtime/helpers/typeof");
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = exports.SkipStep = exports.DoStep = void 0;
exports.isActive = isActive;
var _slicedToArray2 = _interopRequireDefault(require("@babel/runtime/helpers/slicedToArray"));
var _useState3 = _interopRequireDefault(require("rc-util/lib/hooks/useState"));
var React = _interopRequireWildcard(require("react"));
var _interface = require("../interface");
var _useIsomorphicLayoutEffect = _interopRequireDefault(require("./useIsomorphicLayoutEffect"));
var _useNextFrame3 = _interopRequireDefault(require("./useNextFrame"));
function _getRequireWildcardCache(nodeInterop) { if (typeof WeakMap !== "function") return null; var cacheBabelInterop = new WeakMap(); var cacheNodeInterop = new WeakMap(); return (_getRequireWildcardCache = function _getRequireWildcardCache(nodeInterop) { return nodeInterop ? cacheNodeInterop : cacheBabelInterop; })(nodeInterop); }
function _interopRequireWildcard(obj, nodeInterop) { if (!nodeInterop && obj && obj.__esModule) { return obj; } if (obj === null || _typeof(obj) !== "object" && typeof obj !== "function") { return { default: obj }; } var cache = _getRequireWildcardCache(nodeInterop); if (cache && cache.has(obj)) { return cache.get(obj); } var newObj = {}; var hasPropertyDescriptor = Object.defineProperty && Object.getOwnPropertyDescriptor; for (var key in obj) { if (key !== "default" && Object.prototype.hasOwnProperty.call(obj, key)) { var desc = hasPropertyDescriptor ? Object.getOwnPropertyDescriptor(obj, key) : null; if (desc && (desc.get || desc.set)) { Object.defineProperty(newObj, key, desc); } else { newObj[key] = obj[key]; } } } newObj.default = obj; if (cache) { cache.set(obj, newObj); } return newObj; }
var FULL_STEP_QUEUE = [_interface.STEP_PREPARE, _interface.STEP_START, _interface.STEP_ACTIVE, _interface.STEP_ACTIVATED];
var SIMPLE_STEP_QUEUE = [_interface.STEP_PREPARE, _interface.STEP_PREPARED];

/** Skip current step */
var SkipStep = false;
/** Current step should be update in */
exports.SkipStep = SkipStep;
var DoStep = true;
exports.DoStep = DoStep;
function isActive(step) {
  return step === _interface.STEP_ACTIVE || step === _interface.STEP_ACTIVATED;
}
var _default = function _default(status, prepareOnly, callback) {
  var _useState = (0, _useState3.default)(_interface.STEP_NONE),
    _useState2 = (0, _slicedToArray2.default)(_useState, 2),
    step = _useState2[0],
    setStep = _useState2[1];
  var _useNextFrame = (0, _useNextFrame3.default)(),
    _useNextFrame2 = (0, _slicedToArray2.default)(_useNextFrame, 2),
    nextFrame = _useNextFrame2[0],
    cancelNextFrame = _useNextFrame2[1];
  function startQueue() {
    setStep(_interface.STEP_PREPARE, true);
  }
  var STEP_QUEUE = prepareOnly ? SIMPLE_STEP_QUEUE : FULL_STEP_QUEUE;
  (0, _useIsomorphicLayoutEffect.default)(function () {
    if (step !== _interface.STEP_NONE && step !== _interface.STEP_ACTIVATED) {
      var index = STEP_QUEUE.indexOf(step);
      var nextStep = STEP_QUEUE[index + 1];
      var result = callback(step);
      if (result === SkipStep) {
        // Skip when no needed
        setStep(nextStep, true);
      } else if (nextStep) {
        // Do as frame for step update
        nextFrame(function (info) {
          function doNext() {
            // Skip since current queue is ood
            if (info.isCanceled()) return;
            setStep(nextStep, true);
          }
          if (result === true) {
            doNext();
          } else {
            // Only promise should be async
            Promise.resolve(result).then(doNext);
          }
        });
      }
    }
  }, [status, step]);
  React.useEffect(function () {
    return function () {
      cancelNextFrame();
    };
  }, []);
  return [startQueue, step];
};
exports.default = _default;