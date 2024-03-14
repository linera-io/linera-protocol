"use strict";

var _interopRequireWildcard = require("@babel/runtime/helpers/interopRequireWildcard").default;
var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault").default;
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = useAccessibility;
var _KeyCode = _interopRequireDefault(require("rc-util/lib/KeyCode"));
var _raf = _interopRequireDefault(require("rc-util/lib/raf"));
var React = _interopRequireWildcard(require("react"));
var ESC = _KeyCode.default.ESC,
  TAB = _KeyCode.default.TAB;
function useAccessibility(_ref) {
  var visible = _ref.visible,
    triggerRef = _ref.triggerRef,
    onVisibleChange = _ref.onVisibleChange,
    autoFocus = _ref.autoFocus,
    overlayRef = _ref.overlayRef;
  var focusMenuRef = React.useRef(false);
  var handleCloseMenuAndReturnFocus = function handleCloseMenuAndReturnFocus() {
    if (visible) {
      var _triggerRef$current, _triggerRef$current$f;
      (_triggerRef$current = triggerRef.current) === null || _triggerRef$current === void 0 || (_triggerRef$current$f = _triggerRef$current.focus) === null || _triggerRef$current$f === void 0 || _triggerRef$current$f.call(_triggerRef$current);
      onVisibleChange === null || onVisibleChange === void 0 || onVisibleChange(false);
    }
  };
  var focusMenu = function focusMenu() {
    var _overlayRef$current;
    if ((_overlayRef$current = overlayRef.current) !== null && _overlayRef$current !== void 0 && _overlayRef$current.focus) {
      overlayRef.current.focus();
      focusMenuRef.current = true;
      return true;
    }
    return false;
  };
  var handleKeyDown = function handleKeyDown(event) {
    switch (event.keyCode) {
      case ESC:
        handleCloseMenuAndReturnFocus();
        break;
      case TAB:
        {
          var focusResult = false;
          if (!focusMenuRef.current) {
            focusResult = focusMenu();
          }
          if (focusResult) {
            event.preventDefault();
          } else {
            handleCloseMenuAndReturnFocus();
          }
          break;
        }
    }
  };
  React.useEffect(function () {
    if (visible) {
      window.addEventListener("keydown", handleKeyDown);
      if (autoFocus) {
        // FIXME: hack with raf
        (0, _raf.default)(focusMenu, 3);
      }
      return function () {
        window.removeEventListener("keydown", handleKeyDown);
        focusMenuRef.current = false;
      };
    }
    return function () {
      focusMenuRef.current = false;
    };
  }, [visible]); // eslint-disable-line react-hooks/exhaustive-deps
}