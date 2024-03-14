/**
 * What's new?
 * - Common
 *  - [Break] Support special year format, all the year will follow the locale config.
 *  - Blur all of field will trigger `onChange` if validate
 *  - Support `preserveInvalidOnBlur` to not to clean input if invalid and remove `changeOnBlur`
 *  - `pickerValue` is now full controlled
 *    - `defaultPickerValue` will take effect on every field active with popup opening.
 *  - [Break] clear button return the event with `onClick`
 *
 * - Locale
 *  - Remove `dateFormat` since it's never used
 *  - Remove `dateTimeFormat` since it's never used
 *
 * - Picker
 *  - TimePicker support `changeOnScroll`
 *  - TimePicker support `millisecond`
 *  - Support cellMeridiemFormat for AM/PM
 *  - Get correct `disabledHours` when set `use12Hours`
 *  - Support `showWeek`
 *
 * - RangePicker
 *  - [Break] RangePicker is now not limit the range of clicked field.
 *  - Trigger `onCalendarChange` when type correct
 *  - [Break] Not order `value` if given `value` is wrong order.
 *  - Hover `presets` will show date in input field.
 *  - [Break] RangePicker go to end field, `pickerValue` will follow the start field if not controlled.
 */

import RangePicker from "./PickerInput/RangePicker";
import Picker from "./PickerInput/SinglePicker";
import PickerPanel from "./PickerPanel";
export { Picker, RangePicker, PickerPanel };
export default Picker;