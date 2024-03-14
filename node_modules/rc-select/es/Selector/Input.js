import _objectSpread from "@babel/runtime/helpers/esm/objectSpread2";
import * as React from 'react';
import classNames from 'classnames';
import { composeRef } from "rc-util/es/ref";
import { warning } from "rc-util/es/warning";
var Input = function Input(props, ref) {
  var _inputNode2;
  var prefixCls = props.prefixCls,
    id = props.id,
    inputElement = props.inputElement,
    disabled = props.disabled,
    tabIndex = props.tabIndex,
    autoFocus = props.autoFocus,
    autoComplete = props.autoComplete,
    editable = props.editable,
    activeDescendantId = props.activeDescendantId,
    value = props.value,
    maxLength = props.maxLength,
    _onKeyDown = props.onKeyDown,
    _onMouseDown = props.onMouseDown,
    _onChange = props.onChange,
    onPaste = props.onPaste,
    _onCompositionStart = props.onCompositionStart,
    _onCompositionEnd = props.onCompositionEnd,
    open = props.open,
    attrs = props.attrs;
  var inputNode = inputElement || /*#__PURE__*/React.createElement("input", null);
  var _inputNode = inputNode,
    originRef = _inputNode.ref,
    originProps = _inputNode.props;
  var onOriginKeyDown = originProps.onKeyDown,
    onOriginChange = originProps.onChange,
    onOriginMouseDown = originProps.onMouseDown,
    onOriginCompositionStart = originProps.onCompositionStart,
    onOriginCompositionEnd = originProps.onCompositionEnd,
    style = originProps.style;
  warning(!('maxLength' in inputNode.props), "Passing 'maxLength' to input element directly may not work because input in BaseSelect is controlled.");
  inputNode = /*#__PURE__*/React.cloneElement(inputNode, _objectSpread(_objectSpread(_objectSpread({
    type: 'search'
  }, originProps), {}, {
    // Override over origin props
    id: id,
    ref: composeRef(ref, originRef),
    disabled: disabled,
    tabIndex: tabIndex,
    autoComplete: autoComplete || 'off',
    autoFocus: autoFocus,
    className: classNames("".concat(prefixCls, "-selection-search-input"), (_inputNode2 = inputNode) === null || _inputNode2 === void 0 || (_inputNode2 = _inputNode2.props) === null || _inputNode2 === void 0 ? void 0 : _inputNode2.className),
    role: 'combobox',
    'aria-expanded': open || false,
    'aria-haspopup': 'listbox',
    'aria-owns': "".concat(id, "_list"),
    'aria-autocomplete': 'list',
    'aria-controls': "".concat(id, "_list"),
    'aria-activedescendant': open ? activeDescendantId : undefined
  }, attrs), {}, {
    value: editable ? value : '',
    maxLength: maxLength,
    readOnly: !editable,
    unselectable: !editable ? 'on' : null,
    style: _objectSpread(_objectSpread({}, style), {}, {
      opacity: editable ? null : 0
    }),
    onKeyDown: function onKeyDown(event) {
      _onKeyDown(event);
      if (onOriginKeyDown) {
        onOriginKeyDown(event);
      }
    },
    onMouseDown: function onMouseDown(event) {
      _onMouseDown(event);
      if (onOriginMouseDown) {
        onOriginMouseDown(event);
      }
    },
    onChange: function onChange(event) {
      _onChange(event);
      if (onOriginChange) {
        onOriginChange(event);
      }
    },
    onCompositionStart: function onCompositionStart(event) {
      _onCompositionStart(event);
      if (onOriginCompositionStart) {
        onOriginCompositionStart(event);
      }
    },
    onCompositionEnd: function onCompositionEnd(event) {
      _onCompositionEnd(event);
      if (onOriginCompositionEnd) {
        onOriginCompositionEnd(event);
      }
    },
    onPaste: onPaste
  }));
  return inputNode;
};
var RefInput = /*#__PURE__*/React.forwardRef(Input);
if (process.env.NODE_ENV !== 'production') {
  RefInput.displayName = 'Input';
}
export default RefInput;