import { getShadowRoot } from "rc-util/es/Dom/shadow";
import { warning } from "rc-util/es/warning";
import * as React from 'react';
import { getWin } from "../util";
export default function useWinClick(open, clickToHide, targetEle, popupEle, mask, maskClosable, inPopupOrChild, triggerOpen) {
  var openRef = React.useRef(open);
  openRef.current = open;

  // Click to hide is special action since click popup element should not hide
  React.useEffect(function () {
    if (clickToHide && popupEle && (!mask || maskClosable)) {
      var onTriggerClose = function onTriggerClose(_ref) {
        var target = _ref.target;
        if (openRef.current && !inPopupOrChild(target)) {
          triggerOpen(false);
        }
      };
      var win = getWin(popupEle);
      win.addEventListener('mousedown', onTriggerClose, true);
      win.addEventListener('contextmenu', onTriggerClose, true);

      // shadow root
      var targetShadowRoot = getShadowRoot(targetEle);
      if (targetShadowRoot) {
        targetShadowRoot.addEventListener('mousedown', onTriggerClose, true);
        targetShadowRoot.addEventListener('contextmenu', onTriggerClose, true);
      }

      // Warning if target and popup not in same root
      if (process.env.NODE_ENV !== 'production') {
        var _targetEle$getRootNod, _popupEle$getRootNode;
        var targetRoot = targetEle === null || targetEle === void 0 || (_targetEle$getRootNod = targetEle.getRootNode) === null || _targetEle$getRootNod === void 0 ? void 0 : _targetEle$getRootNod.call(targetEle);
        var popupRoot = (_popupEle$getRootNode = popupEle.getRootNode) === null || _popupEle$getRootNode === void 0 ? void 0 : _popupEle$getRootNode.call(popupEle);
        warning(targetRoot === popupRoot, "trigger element and popup element should in same shadow root.");
      }
      return function () {
        win.removeEventListener('mousedown', onTriggerClose, true);
        win.removeEventListener('contextmenu', onTriggerClose, true);
        if (targetShadowRoot) {
          targetShadowRoot.removeEventListener('mousedown', onTriggerClose, true);
          targetShadowRoot.removeEventListener('contextmenu', onTriggerClose, true);
        }
      };
    }
  }, [clickToHide, targetEle, popupEle, mask, maskClosable]);
}