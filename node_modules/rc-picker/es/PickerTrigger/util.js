import { pickProps } from "../utils/miscUtil";
export function pickTriggerProps(props) {
  return pickProps(props, ['placement', 'builtinPlacements', 'popupAlign', 'getPopupContainer', 'transitionName', 'direction']);
}