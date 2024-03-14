import _slicedToArray from "@babel/runtime/helpers/esm/slicedToArray";
import * as React from 'react';
import { PreviewGroupContext } from "../context";
var uid = 0;
export default function useRegisterImage(canPreview, data) {
  var _React$useState = React.useState(function () {
      uid += 1;
      return String(uid);
    }),
    _React$useState2 = _slicedToArray(_React$useState, 1),
    id = _React$useState2[0];
  var groupContext = React.useContext(PreviewGroupContext);
  var registerData = {
    data: data,
    canPreview: canPreview
  };

  // Keep order start
  // Resolve https://github.com/ant-design/ant-design/issues/28881
  // Only need unRegister when component unMount
  React.useEffect(function () {
    if (groupContext) {
      return groupContext.register(id, registerData);
    }
  }, []);
  React.useEffect(function () {
    if (groupContext) {
      groupContext.register(id, registerData);
    }
  }, [canPreview, data]);
  return id;
}