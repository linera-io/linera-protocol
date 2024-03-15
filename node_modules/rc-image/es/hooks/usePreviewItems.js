import _toConsumableArray from "@babel/runtime/helpers/esm/toConsumableArray";
import _defineProperty from "@babel/runtime/helpers/esm/defineProperty";
import _objectSpread from "@babel/runtime/helpers/esm/objectSpread2";
import _slicedToArray from "@babel/runtime/helpers/esm/slicedToArray";
import * as React from 'react';
import { COMMON_PROPS } from "../common";
/**
 * Merge props provided `items` or context collected images
 */
export default function usePreviewItems(items) {
  // Context collection image data
  var _React$useState = React.useState({}),
    _React$useState2 = _slicedToArray(_React$useState, 2),
    images = _React$useState2[0],
    setImages = _React$useState2[1];
  var registerImage = React.useCallback(function (id, data) {
    setImages(function (imgs) {
      return _objectSpread(_objectSpread({}, imgs), {}, _defineProperty({}, id, data));
    });
    return function () {
      setImages(function (imgs) {
        var cloneImgs = _objectSpread({}, imgs);
        delete cloneImgs[id];
        return cloneImgs;
      });
    };
  }, []);

  // items
  var mergedItems = React.useMemo(function () {
    if (items) {
      return items.map(function (item) {
        if (typeof item === 'string') {
          return {
            data: {
              src: item
            }
          };
        }
        var data = {};
        Object.keys(item).forEach(function (key) {
          if (['src'].concat(_toConsumableArray(COMMON_PROPS)).includes(key)) {
            data[key] = item[key];
          }
        });
        return {
          data: data
        };
      });
    }
    return Object.keys(images).reduce(function (total, id) {
      var _images$id = images[id],
        canPreview = _images$id.canPreview,
        data = _images$id.data;
      if (canPreview) {
        total.push({
          data: data,
          id: id
        });
      }
      return total;
    }, []);
  }, [items, images]);
  return [mergedItems, registerImage];
}