import * as React from 'react';
var SliderContext = /*#__PURE__*/React.createContext({
  min: 0,
  max: 0,
  direction: 'ltr',
  step: 1,
  includedStart: 0,
  includedEnd: 0,
  tabIndex: 0,
  keyboard: true,
  styles: {},
  classNames: {}
});
export default SliderContext;