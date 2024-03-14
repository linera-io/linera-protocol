"use strict";
"use client";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault").default;
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = void 0;
var _dayjs = _interopRequireDefault(require("rc-picker/lib/generate/dayjs"));
var _PurePanel = _interopRequireDefault(require("../_util/PurePanel"));
var _generatePicker = _interopRequireDefault(require("./generatePicker"));
var _util = require("./util");
const DatePicker = (0, _generatePicker.default)(_dayjs.default);
function postPureProps(props) {
  const dropdownAlign = (0, _util.transPlacement2DropdownAlign)(props.direction, props.placement);
  dropdownAlign.overflow.adjustY = false;
  dropdownAlign.overflow.adjustX = false;
  return Object.assign(Object.assign({}, props), {
    dropdownAlign
  });
}
// We don't care debug panel
/* istanbul ignore next */
const PurePanel = (0, _PurePanel.default)(DatePicker, 'picker', null, postPureProps);
DatePicker._InternalPanelDoNotUseOrYouWillBeFired = PurePanel;
const PureRangePanel = (0, _PurePanel.default)(DatePicker.RangePicker, 'picker', null, postPureProps);
DatePicker._InternalRangePanelDoNotUseOrYouWillBeFired = PureRangePanel;
DatePicker.generatePicker = _generatePicker.default;
var _default = exports.default = DatePicker;