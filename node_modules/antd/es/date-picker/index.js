"use client";

import dayjsGenerateConfig from "rc-picker/es/generate/dayjs";
import genPurePanel from '../_util/PurePanel';
import generatePicker from './generatePicker';
import { transPlacement2DropdownAlign } from './util';
const DatePicker = generatePicker(dayjsGenerateConfig);
function postPureProps(props) {
  const dropdownAlign = transPlacement2DropdownAlign(props.direction, props.placement);
  dropdownAlign.overflow.adjustY = false;
  dropdownAlign.overflow.adjustX = false;
  return Object.assign(Object.assign({}, props), {
    dropdownAlign
  });
}
// We don't care debug panel
/* istanbul ignore next */
const PurePanel = genPurePanel(DatePicker, 'picker', null, postPureProps);
DatePicker._InternalPanelDoNotUseOrYouWillBeFired = PurePanel;
const PureRangePanel = genPurePanel(DatePicker.RangePicker, 'picker', null, postPureProps);
DatePicker._InternalRangePanelDoNotUseOrYouWillBeFired = PureRangePanel;
DatePicker.generatePicker = generatePicker;
export default DatePicker;