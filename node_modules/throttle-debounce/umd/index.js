(function (global, factory) {
	typeof exports === 'object' && typeof module !== 'undefined' ? factory(exports) :
	typeof define === 'function' && define.amd ? define(['exports'], factory) :
	(global = typeof globalThis !== 'undefined' ? globalThis : global || self, factory(global.throttleDebounce = {}));
})(this, (function (exports) { 'use strict';

	/* eslint-disable no-undefined,no-param-reassign,no-shadow */

	/**
	 * Throttle execution of a function. Especially useful for rate limiting
	 * execution of handlers on events like resize and scroll.
	 *
	 * @param {number} delay -                  A zero-or-greater delay in milliseconds. For event callbacks, values around 100 or 250 (or even higher)
	 *                                            are most useful.
	 * @param {Function} callback -               A function to be executed after delay milliseconds. The `this` context and all arguments are passed through,
	 *                                            as-is, to `callback` when the throttled-function is executed.
	 * @param {object} [options] -              An object to configure options.
	 * @param {boolean} [options.noTrailing] -   Optional, defaults to false. If noTrailing is true, callback will only execute every `delay` milliseconds
	 *                                            while the throttled-function is being called. If noTrailing is false or unspecified, callback will be executed
	 *                                            one final time after the last throttled-function call. (After the throttled-function has not been called for
	 *                                            `delay` milliseconds, the internal counter is reset).
	 * @param {boolean} [options.noLeading] -   Optional, defaults to false. If noLeading is false, the first throttled-function call will execute callback
	 *                                            immediately. If noLeading is true, the first the callback execution will be skipped. It should be noted that
	 *                                            callback will never executed if both noLeading = true and noTrailing = true.
	 * @param {boolean} [options.debounceMode] - If `debounceMode` is true (at begin), schedule `clear` to execute after `delay` ms. If `debounceMode` is
	 *                                            false (at end), schedule `callback` to execute after `delay` ms.
	 *
	 * @returns {Function} A new, throttled, function.
	 */
	function throttle (delay, callback, options) {
	  var _ref = options || {},
	      _ref$noTrailing = _ref.noTrailing,
	      noTrailing = _ref$noTrailing === void 0 ? false : _ref$noTrailing,
	      _ref$noLeading = _ref.noLeading,
	      noLeading = _ref$noLeading === void 0 ? false : _ref$noLeading,
	      _ref$debounceMode = _ref.debounceMode,
	      debounceMode = _ref$debounceMode === void 0 ? undefined : _ref$debounceMode;
	  /*
	   * After wrapper has stopped being called, this timeout ensures that
	   * `callback` is executed at the proper times in `throttle` and `end`
	   * debounce modes.
	   */


	  var timeoutID;
	  var cancelled = false; // Keep track of the last time `callback` was executed.

	  var lastExec = 0; // Function to clear existing timeout

	  function clearExistingTimeout() {
	    if (timeoutID) {
	      clearTimeout(timeoutID);
	    }
	  } // Function to cancel next exec


	  function cancel(options) {
	    var _ref2 = options || {},
	        _ref2$upcomingOnly = _ref2.upcomingOnly,
	        upcomingOnly = _ref2$upcomingOnly === void 0 ? false : _ref2$upcomingOnly;

	    clearExistingTimeout();
	    cancelled = !upcomingOnly;
	  }
	  /*
	   * The `wrapper` function encapsulates all of the throttling / debouncing
	   * functionality and when executed will limit the rate at which `callback`
	   * is executed.
	   */


	  function wrapper() {
	    for (var _len = arguments.length, arguments_ = new Array(_len), _key = 0; _key < _len; _key++) {
	      arguments_[_key] = arguments[_key];
	    }

	    var self = this;
	    var elapsed = Date.now() - lastExec;

	    if (cancelled) {
	      return;
	    } // Execute `callback` and update the `lastExec` timestamp.


	    function exec() {
	      lastExec = Date.now();
	      callback.apply(self, arguments_);
	    }
	    /*
	     * If `debounceMode` is true (at begin) this is used to clear the flag
	     * to allow future `callback` executions.
	     */


	    function clear() {
	      timeoutID = undefined;
	    }

	    if (!noLeading && debounceMode && !timeoutID) {
	      /*
	       * Since `wrapper` is being called for the first time and
	       * `debounceMode` is true (at begin), execute `callback`
	       * and noLeading != true.
	       */
	      exec();
	    }

	    clearExistingTimeout();

	    if (debounceMode === undefined && elapsed > delay) {
	      if (noLeading) {
	        /*
	         * In throttle mode with noLeading, if `delay` time has
	         * been exceeded, update `lastExec` and schedule `callback`
	         * to execute after `delay` ms.
	         */
	        lastExec = Date.now();

	        if (!noTrailing) {
	          timeoutID = setTimeout(debounceMode ? clear : exec, delay);
	        }
	      } else {
	        /*
	         * In throttle mode without noLeading, if `delay` time has been exceeded, execute
	         * `callback`.
	         */
	        exec();
	      }
	    } else if (noTrailing !== true) {
	      /*
	       * In trailing throttle mode, since `delay` time has not been
	       * exceeded, schedule `callback` to execute `delay` ms after most
	       * recent execution.
	       *
	       * If `debounceMode` is true (at begin), schedule `clear` to execute
	       * after `delay` ms.
	       *
	       * If `debounceMode` is false (at end), schedule `callback` to
	       * execute after `delay` ms.
	       */
	      timeoutID = setTimeout(debounceMode ? clear : exec, debounceMode === undefined ? delay - elapsed : delay);
	    }
	  }

	  wrapper.cancel = cancel; // Return the wrapper function.

	  return wrapper;
	}

	/* eslint-disable no-undefined */
	/**
	 * Debounce execution of a function. Debouncing, unlike throttling,
	 * guarantees that a function is only executed a single time, either at the
	 * very beginning of a series of calls, or at the very end.
	 *
	 * @param {number} delay -               A zero-or-greater delay in milliseconds. For event callbacks, values around 100 or 250 (or even higher) are most useful.
	 * @param {Function} callback -          A function to be executed after delay milliseconds. The `this` context and all arguments are passed through, as-is,
	 *                                        to `callback` when the debounced-function is executed.
	 * @param {object} [options] -           An object to configure options.
	 * @param {boolean} [options.atBegin] -  Optional, defaults to false. If atBegin is false or unspecified, callback will only be executed `delay` milliseconds
	 *                                        after the last debounced-function call. If atBegin is true, callback will be executed only at the first debounced-function call.
	 *                                        (After the throttled-function has not been called for `delay` milliseconds, the internal counter is reset).
	 *
	 * @returns {Function} A new, debounced function.
	 */

	function debounce (delay, callback, options) {
	  var _ref = options || {},
	      _ref$atBegin = _ref.atBegin,
	      atBegin = _ref$atBegin === void 0 ? false : _ref$atBegin;

	  return throttle(delay, callback, {
	    debounceMode: atBegin !== false
	  });
	}

	exports.debounce = debounce;
	exports.throttle = throttle;

	Object.defineProperty(exports, '__esModule', { value: true });

}));
//# sourceMappingURL=index.js.map
