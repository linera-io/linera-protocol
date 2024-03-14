import isVisible from "rc-util/es/Dom/isVisible";
import KeyCode from "rc-util/es/KeyCode";
import raf from "rc-util/es/raf";
var scrollIds = new Map();

/** Trigger when element is visible in view */
export function waitElementReady(element, callback) {
  var id;
  function tryOrNextFrame() {
    if (isVisible(element)) {
      callback();
    } else {
      id = raf(function () {
        tryOrNextFrame();
      });
    }
  }
  tryOrNextFrame();
  return function () {
    raf.cancel(id);
  };
}

/* eslint-disable no-param-reassign */
export function scrollTo(element, to, duration) {
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

export function createKeyDownHandler(event, _ref) {
  var onLeftRight = _ref.onLeftRight,
    onCtrlLeftRight = _ref.onCtrlLeftRight,
    onUpDown = _ref.onUpDown,
    onPageUpDown = _ref.onPageUpDown,
    onEnter = _ref.onEnter;
  var which = event.which,
    ctrlKey = event.ctrlKey,
    metaKey = event.metaKey;
  switch (which) {
    case KeyCode.LEFT:
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
    case KeyCode.RIGHT:
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
    case KeyCode.UP:
      if (onUpDown) {
        onUpDown(-1);
        return true;
      }
      /* istanbul ignore next */
      break;
    case KeyCode.DOWN:
      if (onUpDown) {
        onUpDown(1);
        return true;
      }
      /* istanbul ignore next */
      break;
    case KeyCode.PAGE_UP:
      if (onPageUpDown) {
        onPageUpDown(-1);
        return true;
      }
      /* istanbul ignore next */
      break;
    case KeyCode.PAGE_DOWN:
      if (onPageUpDown) {
        onPageUpDown(1);
        return true;
      }
      /* istanbul ignore next */
      break;
    case KeyCode.ENTER:
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
export function getDefaultFormat(format, picker, showTime, use12Hours) {
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
export function elementsContains(elements, target) {
  return elements.some(function (ele) {
    return ele && ele.contains(target);
  });
}