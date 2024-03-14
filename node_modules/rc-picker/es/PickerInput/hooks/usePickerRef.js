import * as React from 'react';
export default function usePickerRef(ref) {
  var selectorRef = React.useRef();
  React.useImperativeHandle(ref, function () {
    var _selectorRef$current;
    return {
      nativeElement: (_selectorRef$current = selectorRef.current) === null || _selectorRef$current === void 0 ? void 0 : _selectorRef$current.nativeElement,
      focus: function focus(options) {
        var _selectorRef$current2;
        (_selectorRef$current2 = selectorRef.current) === null || _selectorRef$current2 === void 0 || _selectorRef$current2.focus(options);
      },
      blur: function blur() {
        var _selectorRef$current3;
        (_selectorRef$current3 = selectorRef.current) === null || _selectorRef$current3 === void 0 || _selectorRef$current3.blur();
      }
    };
  });
  return selectorRef;
}