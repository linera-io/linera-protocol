/**
 * Since Select, TreeSelect, Cascader is same Select like component.
 * We just use same hook to handle this logic.
 *
 * If `suffixIcon` is not equal to `null`, always show it.
 */
export default function useShowArrow(suffixIcon, showArrow) {
  return showArrow !== undefined ? showArrow : suffixIcon !== null;
}