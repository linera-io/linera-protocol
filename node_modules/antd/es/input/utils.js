// eslint-disable-next-line import/prefer-default-export
export function hasPrefixSuffix(props) {
  return !!(props.prefix || props.suffix || props.allowClear || props.showCount);
}