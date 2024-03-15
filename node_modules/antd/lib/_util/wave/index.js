"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault").default;
var _interopRequireWildcard = require("@babel/runtime/helpers/interopRequireWildcard").default;
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = void 0;
var _react = _interopRequireWildcard(require("react"));
var _classnames = _interopRequireDefault(require("classnames"));
var _isVisible = _interopRequireDefault(require("rc-util/lib/Dom/isVisible"));
var _ref = require("rc-util/lib/ref");
var _configProvider = require("../../config-provider");
var _reactNode = require("../reactNode");
var _style = _interopRequireDefault(require("./style"));
var _useWave = _interopRequireDefault(require("./useWave"));
const Wave = props => {
  const {
    children,
    disabled,
    component
  } = props;
  const {
    getPrefixCls
  } = (0, _react.useContext)(_configProvider.ConfigContext);
  const containerRef = (0, _react.useRef)(null);
  // ============================== Style ===============================
  const prefixCls = getPrefixCls('wave');
  const [, hashId] = (0, _style.default)(prefixCls);
  // =============================== Wave ===============================
  const showWave = (0, _useWave.default)(containerRef, (0, _classnames.default)(prefixCls, hashId), component);
  // ============================== Effect ==============================
  _react.default.useEffect(() => {
    const node = containerRef.current;
    if (!node || node.nodeType !== 1 || disabled) {
      return;
    }
    // Click handler
    const onClick = e => {
      // Fix radio button click twice
      if (!(0, _isVisible.default)(e.target) ||
      // No need wave
      !node.getAttribute || node.getAttribute('disabled') || node.disabled || node.className.includes('disabled') || node.className.includes('-leave')) {
        return;
      }
      showWave(e);
    };
    // Bind events
    node.addEventListener('click', onClick, true);
    return () => {
      node.removeEventListener('click', onClick, true);
    };
  }, [disabled]);
  // ============================== Render ==============================
  if (! /*#__PURE__*/_react.default.isValidElement(children)) {
    return children !== null && children !== void 0 ? children : null;
  }
  const ref = (0, _ref.supportRef)(children) ? (0, _ref.composeRef)(children.ref, containerRef) : containerRef;
  return (0, _reactNode.cloneElement)(children, {
    ref
  });
};
if (process.env.NODE_ENV !== 'production') {
  Wave.displayName = 'Wave';
}
var _default = exports.default = Wave;