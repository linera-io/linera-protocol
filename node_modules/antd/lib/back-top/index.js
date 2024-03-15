"use strict";
"use client";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault").default;
var _interopRequireWildcard = require("@babel/runtime/helpers/interopRequireWildcard").default;
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = void 0;
var React = _interopRequireWildcard(require("react"));
var _VerticalAlignTopOutlined = _interopRequireDefault(require("@ant-design/icons/VerticalAlignTopOutlined"));
var _classnames = _interopRequireDefault(require("classnames"));
var _rcMotion = _interopRequireDefault(require("rc-motion"));
var _omit = _interopRequireDefault(require("rc-util/lib/omit"));
var _getScroll = _interopRequireDefault(require("../_util/getScroll"));
var _reactNode = require("../_util/reactNode");
var _scrollTo = _interopRequireDefault(require("../_util/scrollTo"));
var _throttleByAnimationFrame = _interopRequireDefault(require("../_util/throttleByAnimationFrame"));
var _warning = require("../_util/warning");
var _configProvider = require("../config-provider");
var _style = _interopRequireDefault(require("./style"));
const BackTop = props => {
  const {
    prefixCls: customizePrefixCls,
    className,
    rootClassName,
    visibilityHeight = 400,
    target,
    onClick,
    duration = 450
  } = props;
  const [visible, setVisible] = React.useState(visibilityHeight === 0);
  const ref = React.useRef(null);
  const getDefaultTarget = () => ref.current && ref.current.ownerDocument ? ref.current.ownerDocument : window;
  const handleScroll = (0, _throttleByAnimationFrame.default)(e => {
    const scrollTop = (0, _getScroll.default)(e.target, true);
    setVisible(scrollTop >= visibilityHeight);
  });
  if (process.env.NODE_ENV !== 'production') {
    const warning = (0, _warning.devUseWarning)('BackTop');
    warning.deprecated(false, 'BackTop', 'FloatButton.BackTop');
  }
  React.useEffect(() => {
    const getTarget = target || getDefaultTarget;
    const container = getTarget();
    handleScroll({
      target: container
    });
    container === null || container === void 0 ? void 0 : container.addEventListener('scroll', handleScroll);
    return () => {
      handleScroll.cancel();
      container === null || container === void 0 ? void 0 : container.removeEventListener('scroll', handleScroll);
    };
  }, [target]);
  const scrollToTop = e => {
    (0, _scrollTo.default)(0, {
      getContainer: target || getDefaultTarget,
      duration
    });
    onClick === null || onClick === void 0 ? void 0 : onClick(e);
  };
  const {
    getPrefixCls,
    direction
  } = React.useContext(_configProvider.ConfigContext);
  const prefixCls = getPrefixCls('back-top', customizePrefixCls);
  const rootPrefixCls = getPrefixCls();
  const [wrapCSSVar, hashId, cssVarCls] = (0, _style.default)(prefixCls);
  const classString = (0, _classnames.default)(hashId, cssVarCls, prefixCls, {
    [`${prefixCls}-rtl`]: direction === 'rtl'
  }, className, rootClassName);
  // fix https://fb.me/react-unknown-prop
  const divProps = (0, _omit.default)(props, ['prefixCls', 'className', 'rootClassName', 'children', 'visibilityHeight', 'target']);
  const defaultElement = /*#__PURE__*/React.createElement("div", {
    className: `${prefixCls}-content`
  }, /*#__PURE__*/React.createElement("div", {
    className: `${prefixCls}-icon`
  }, /*#__PURE__*/React.createElement(_VerticalAlignTopOutlined.default, null)));
  return wrapCSSVar( /*#__PURE__*/React.createElement("div", Object.assign({}, divProps, {
    className: classString,
    onClick: scrollToTop,
    ref: ref
  }), /*#__PURE__*/React.createElement(_rcMotion.default, {
    visible: visible,
    motionName: `${rootPrefixCls}-fade`
  }, _ref => {
    let {
      className: motionClassName
    } = _ref;
    return (0, _reactNode.cloneElement)(props.children || defaultElement, _ref2 => {
      let {
        className: cloneCls
      } = _ref2;
      return {
        className: (0, _classnames.default)(motionClassName, cloneCls)
      };
    });
  })));
};
if (process.env.NODE_ENV !== 'production') {
  BackTop.displayName = 'BackTop';
}
var _default = exports.default = BackTop;