"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault");
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.createKeyDownHandler = createKeyDownHandler;
exports.elementsContains = elementsContains;
exports.getDefaultFormat = getDefaultFormat;
exports.scrollTo = scrollTo;
exports.waitElementReady = waitElementReady;
var _isVisible = _interopRequireDefault(require("rc-util/lib/Dom/isVisible"));
var _KeyCode = _interopRequireDefault(require("rc-util/lib/KeyCode"));
var _raf = _interopRequireDefault(require("rc-util/lib/raf"));
var scrollIds = new Map();

/** Trigger when element is visible in view */
function waitElementReady(element, callback) {
  var id;
  function tryOrNextFrame() {
    if ((0, _isVisible.default)(element)) {
      callback();
    } else {
      id = (0, _raf.default)(function () {
        tryOrNextFrame();
      });
    }
  }
  tryOrNextFrame();
  return function () {
    _raf.default.cancel(id);
  };
}

/* eslint-disable no-param-reassign */
function scrollTo(element, to, duration) {
  if (scrollIds.get(element)) {
    cancelAnimationFrame(scrollIds.get(element));
  }

  // jump to target if duration zero
  if (duration <= 0) {
    scrollIds.set(element, requestAnimationFrame(function () {
      element.scrollTop = to;
    }));
    return;
  }
  var difference = to - element.scrollTop;
  var perTick = difference / duration * 10;
  scrollIds.set(element, requestAnimationFrame(function () {
    element.scrollTop += perTick;
    if (element.scrollTop !== to) {
      scrollTo(element, to, duration - 10);
    }
  }));
}
/* eslint-enable */

function createKeyDownHandler(event, _ref) {
  var onLeftRight = _ref.onLeftRight,
    onCtrlLeftRight = _ref.onCtrlLeftRight,
    onUpDown = _ref.onUpDown,
    onPageUpDown = _ref.onPageUpDown,
    onEnter = _ref.onEnter;
  var which = event.which,
    ctrlKey = event.ctrlKey,
    metaKey = event.metaKey;
  switch (which) {
    case _KeyCode.default.LEFT:
      if (ctrlKey || metaKey) {
        if (onCtrlLeftRight) {
          onCtrlLeftRight(-1);
          return true;
        }
      } else if (onLeftRight) {
        onLeftRight(-1);
        return true;
      }
      /* istanbul ignore next */
      break;
    case _KeyCode.default.RIGHT:
      if (ctrlKey || metaKey) {
        if (onCtrlLeftRight) {
          onCtrlLeftRight(1);
          return true;
        }
      } else if (onLeftRight) {
        onLeftRight(1);
        return true;
      }
      /* istanbul ignore next */
      break;
    case _KeyCode.default.UP:
      if (onUpDown) {
        onUpDown(-1);
        return true;
      }
      /* istanbul ignore next */
      break;
    case _KeyCode.default.DOWN:
      if (onUpDown) {
        onUpDown(1);
        return true;
      }
      /* istanbul ignore next */
      break;
    case _KeyCode.default.PAGE_UP:
      if (onPageUpDown) {
        onPageUpDown(-1);
        return true;
      }
      /* istanbul ignore next */
      break;
    case _KeyCode.default.PAGE_DOWN:
      if (onPageUpDown) {
        onPageUpDown(1);
        return true;
      }
      /* istanbul ignore next */
      break;
    case _KeyCode.default.ENTER:
      if (onEnter) {
        onEnter();
        return true;
      }
      /* istanbul ignore next */
      break;
  }
  return false;
}

// ===================== Format =====================
function getDefaultFormat(format, picker, showTime, use12Hours) {
  var mergedFormat = format;
  if (!mergedFormat) {
    switch (picker) {
      case 'time':
        mergedFormat = use12Hours ? 'hh:mm:ss a' : 'HH:mm:ss';
        break;
      case 'week':
        mergedFormat = 'gggg-wo';
        break;
      case 'month':
        mergedFormat = 'YYYY-MM';
        break;
      case 'quarter':
        mergedFormat = 'YYYY-[Q]Q';
        break;
      case 'year':
        mergedFormat = 'YYYY';
        break;
      default:
        mergedFormat = showTime ? 'YYYY-MM-DD HH:mm:ss' : 'YYYY-MM-DD';
    }
  }
  return mergedFormat;
}

// ====================== Mode ======================
function elementsContains(elements, target) {
  return elements.some(function (ele) {
    return ele && ele.contains(target);
  });
}