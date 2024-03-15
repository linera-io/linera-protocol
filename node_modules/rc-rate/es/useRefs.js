import * as React from 'react';
export default function useRefs() {
  var nodeRef = React.useRef({});
  function getRef(index) {
    return nodeRef.current[index];
  }
  function setRef(index) {
    return function (node) {
      nodeRef.current[index] = node;
    };
  }
  return [getRef, setRef];
}