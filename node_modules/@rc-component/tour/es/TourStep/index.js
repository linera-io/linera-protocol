import * as React from 'react';
import DefaultPanel from "./DefaultPanel";
var TourStep = function TourStep(props) {
  var current = props.current,
    renderPanel = props.renderPanel;
  return /*#__PURE__*/React.createElement(React.Fragment, null, typeof renderPanel === 'function' ? renderPanel(props, current) : /*#__PURE__*/React.createElement(DefaultPanel, props));
};
export default TourStep;