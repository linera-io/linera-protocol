import { useEvent } from 'rc-util';
import raf from "rc-util/es/raf";
import isVisible from "rc-util/es/Dom/isVisible";
import * as React from 'react';
var SPEED_PTG = 1 / 3;
export default function useScrollTo(ulRef, value) {
  // ========================= Scroll =========================
  var scrollingRef = React.useRef(false);
  var scrollRafRef = React.useRef(null);
  var scrollDistRef = React.useRef(null);
  var isScrolling = function isScrolling() {
    return scrollingRef.current;
  };
  var stopScroll = function stopScroll() {
    raf.cancel(scrollRafRef.current);
    scrollingRef.current = false;
  };
  var scrollRafTimesRef = React.useRef();
  var startScroll = function startScroll() {
    var ul = ulRef.current;
    scrollDistRef.current = null;
    scrollRafTimesRef.current = 0;
    if (ul) {
      var targetLi = ul.querySelector("[data-value=\"".concat(value, "\"]"));
      var firstLi = ul.querySelector("li");
      var doScroll = function doScroll() {
        stopScroll();
        scrollingRef.current = true;
        scrollRafTimesRef.current += 1;
        var currentTop = ul.scrollTop;
        var firstLiTop = firstLi.offsetTop;
        var targetLiTop = targetLi.offsetTop;
        var targetTop = targetLiTop - firstLiTop;

        // Wait for element exist. 5 frames is enough
        if (targetLiTop === 0 && targetLi !== firstLi || !isVisible(ul)) {
          if (scrollRafTimesRef.current <= 5) {
            scrollRafRef.current = raf(doScroll);
          }
          return;
        }
        var nextTop = currentTop + (targetTop - currentTop) * SPEED_PTG;
        var dist = Math.abs(targetTop - nextTop);

        // Break if dist get larger, which means user is scrolling
        if (scrollDistRef.current !== null && scrollDistRef.current < dist) {
          stopScroll();
          return;
        }
        scrollDistRef.current = dist;

        // Stop when dist is less than 1
        if (dist <= 1) {
          ul.scrollTop = targetTop;
          stopScroll();
          return;
        }

        // IE not support `scrollTo`
        ul.scrollTop = nextTop;
        scrollRafRef.current = raf(doScroll);
      };
      if (targetLi && firstLi) {
        doScroll();
      }
    }
  };

  // ======================== Trigger =========================
  var syncScroll = useEvent(startScroll);
  return [syncScroll, stopScroll, isScrolling];
}