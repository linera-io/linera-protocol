import _toConsumableArray from "@babel/runtime/helpers/esm/toConsumableArray";
import * as React from 'react';
export default function usePatchElement() {
  const [elements, setElements] = React.useState([]);
  const patchElement = React.useCallback(element => {
    // append a new element to elements (and create a new ref)
    setElements(originElements => [].concat(_toConsumableArray(originElements), [element]));
    // return a function that removes the new element out of elements (and create a new ref)
    // it works a little like useEffect
    return () => {
      setElements(originElements => originElements.filter(ele => ele !== element));
    };
  }, []);
  return [elements, patchElement];
}