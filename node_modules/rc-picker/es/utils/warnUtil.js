import warning from "rc-util/es/warning";
export function legacyPropsWarning(props) {
  var picker = props.picker,
    disabledHours = props.disabledHours,
    disabledMinutes = props.disabledMinutes,
    disabledSeconds = props.disabledSeconds;
  if (picker === 'time' && (disabledHours || disabledMinutes || disabledSeconds)) {
    warning(false, "'disabledHours', 'disabledMinutes', 'disabledSeconds' will be removed in the next major version, please use 'disabledTime' instead.");
  }
}