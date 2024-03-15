import _slicedToArray from "@babel/runtime/helpers/esm/slicedToArray";
import * as React from 'react';
import { useRef, useEffect } from 'react';
import findDOMNode from "rc-util/es/Dom/findDOMNode";
import raf from "rc-util/es/raf";
import CacheMap from "../utils/CacheMap";
export default function useHeights(getKey, onItemAdd, onItemRemove) {
  var _React$useState = React.useState(0),
    _React$useState2 = _slicedToArray(_React$useState, 2),
    updatedMark = _React$useState2[0],
    setUpdatedMark = _React$useState2[1];
  var instanceRef = useRef(new Map());
  var heightsRef = useRef(new CacheMap());
  var collectRafRef = useRef();
  function cancelRaf() {
    raf.cancel(collectRafRef.current);
  }
  function collectHeight() {
    var sync = arguments.length > 0 && arguments[0] !== undefined ? arguments[0] : false;
    cancelRaf();
    var doCollect = function doCollect() {
      instanceRef.current.forEach(function (element, key) {
        if (element && element.offsetParent) {
          var htmlElement = findDOMNode(element);
          var offsetHeight = htmlElement.offsetHeight;
          if (heightsRef.current.get(key) !== offsetHeight) {
            heightsRef.current.set(key, htmlElement.offsetHeight);
          }
        }
      });

      // Always trigger update mark to tell parent that should re-calculate heights when resized
      setUpdatedMark(function (c) {
        return c + 1;
      });
    };
    if (sync) {
      doCollect();
    } else {
      collectRafRef.current = raf(doCollect);
    }
  }
  function setInstanceRef(item, instance) {
    var key = getKey(item);
    var origin = instanceRef.current.get(key);
    if (instance) {
      instanceRef.current.set(key, instance);
      collectHeight();
    } else {
      instanceRef.current.delete(key);
    }

    // Instance changed
    if (!origin !== !instance) {
      if (instance) {
        onItemAdd === null || onItemAdd === void 0 || onItemAdd(item);
      } else {
        onItemRemove === null || onItemRemove === void 0 || onItemRemove(item);
      }
    }
  }
  useEffect(function () {
    return cancelRaf;
  }, []);
  return [setInstanceRef, collectHeight, heightsRef.current, updatedMark];
}