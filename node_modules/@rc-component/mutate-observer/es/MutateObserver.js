import _slicedToArray from "@babel/runtime/helpers/esm/slicedToArray";
import React from 'react';
import useLayoutEffect from "rc-util/es/hooks/useLayoutEffect";
import { supportRef, useComposeRef } from "rc-util/es/ref";
import findDOMNode from "rc-util/es/Dom/findDOMNode";
import useEvent from "rc-util/es/hooks/useEvent";
import DomWrapper from "./wrapper";
import useMutateObserver from "./useMutateObserver";
var MutateObserver = function MutateObserver(props) {
  var children = props.children,
    options = props.options,
    _props$onMutate = props.onMutate,
    onMutate = _props$onMutate === void 0 ? function () {} : _props$onMutate;
  var callback = useEvent(onMutate);
  var wrapperRef = React.useRef(null);
  var elementRef = React.useRef(null);
  var canRef = /*#__PURE__*/React.isValidElement(children) && supportRef(children);
  var mergedRef = useComposeRef(elementRef, canRef ? children.ref : null);
  var _React$useState = React.useState(null),
    _React$useState2 = _slicedToArray(_React$useState, 2),
    target = _React$useState2[0],
    setTarget = _React$useState2[1];
  useMutateObserver(target, callback, options);

  // =========================== Effect ===========================
  // Bind target
  useLayoutEffect(function () {
    setTarget(findDOMNode(elementRef.current) || findDOMNode(wrapperRef.current));
  });

  // =========================== Render ===========================
  if (!children) {
    if (process.env.NODE_ENV !== 'production') {
      console.error('MutationObserver need children props');
    }
    return null;
  }
  return /*#__PURE__*/React.createElement(DomWrapper, {
    ref: wrapperRef
  }, canRef ? /*#__PURE__*/React.cloneElement(children, {
    ref: mergedRef
  }) : children);
};
export default MutateObserver;