"use strict";
"use client";

var _interopRequireWildcard = require("@babel/runtime/helpers/interopRequireWildcard").default;
var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault").default;
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = void 0;
var _EnterOutlined = _interopRequireDefault(require("@ant-design/icons/EnterOutlined"));
var _classnames = _interopRequireDefault(require("classnames"));
var _KeyCode = _interopRequireDefault(require("rc-util/lib/KeyCode"));
var React = _interopRequireWildcard(require("react"));
var _reactNode = require("../_util/reactNode");
var _TextArea = _interopRequireDefault(require("../input/TextArea"));
var _style = _interopRequireDefault(require("./style"));
const Editable = props => {
  const {
    prefixCls,
    'aria-label': ariaLabel,
    className,
    style,
    direction,
    maxLength,
    autoSize = true,
    value,
    onSave,
    onCancel,
    onEnd,
    component,
    enterIcon = /*#__PURE__*/React.createElement(_EnterOutlined.default, null)
  } = props;
  const ref = React.useRef(null);
  const inComposition = React.useRef(false);
  const lastKeyCode = React.useRef();
  const [current, setCurrent] = React.useState(value);
  React.useEffect(() => {
    setCurrent(value);
  }, [value]);
  React.useEffect(() => {
    if (ref.current && ref.current.resizableTextArea) {
      const {
        textArea
      } = ref.current.resizableTextArea;
      textArea.focus();
      const {
        length
      } = textArea.value;
      textArea.setSelectionRange(length, length);
    }
  }, []);
  const onChange = _ref => {
    let {
      target
    } = _ref;
    setCurrent(target.value.replace(/[\n\r]/g, ''));
  };
  const onCompositionStart = () => {
    inComposition.current = true;
  };
  const onCompositionEnd = () => {
    inComposition.current = false;
  };
  const onKeyDown = _ref2 => {
    let {
      keyCode
    } = _ref2;
    // We don't record keyCode when IME is using
    if (inComposition.current) return;
    lastKeyCode.current = keyCode;
  };
  const confirmChange = () => {
    onSave(current.trim());
  };
  const onKeyUp = _ref3 => {
    let {
      keyCode,
      ctrlKey,
      altKey,
      metaKey,
      shiftKey
    } = _ref3;
    // Check if it's a real key
    if (lastKeyCode.current === keyCode && !inComposition.current && !ctrlKey && !altKey && !metaKey && !shiftKey) {
      if (keyCode === _KeyCode.default.ENTER) {
        confirmChange();
        onEnd === null || onEnd === void 0 ? void 0 : onEnd();
      } else if (keyCode === _KeyCode.default.ESC) {
        onCancel();
      }
    }
  };
  const onBlur = () => {
    confirmChange();
  };
  const textClassName = component ? `${prefixCls}-${component}` : '';
  const [wrapCSSVar, hashId, cssVarCls] = (0, _style.default)(prefixCls);
  const textAreaClassName = (0, _classnames.default)(prefixCls, `${prefixCls}-edit-content`, {
    [`${prefixCls}-rtl`]: direction === 'rtl'
  }, className, textClassName, hashId, cssVarCls);
  return wrapCSSVar( /*#__PURE__*/React.createElement("div", {
    className: textAreaClassName,
    style: style
  }, /*#__PURE__*/React.createElement(_TextArea.default, {
    ref: ref,
    maxLength: maxLength,
    value: current,
    onChange: onChange,
    onKeyDown: onKeyDown,
    onKeyUp: onKeyUp,
    onCompositionStart: onCompositionStart,
    onCompositionEnd: onCompositionEnd,
    onBlur: onBlur,
    "aria-label": ariaLabel,
    rows: 1,
    autoSize: autoSize
  }), enterIcon !== null ? (0, _reactNode.cloneElement)(enterIcon, {
    className: `${prefixCls}-edit-content-confirm`
  }) : null));
};
var _default = exports.default = Editable;