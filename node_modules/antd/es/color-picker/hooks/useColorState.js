import { useEffect, useState } from 'react';
import { generateColor } from '../util';
function hasValue(value) {
  return value !== undefined;
}
const useColorState = (defaultStateValue, option) => {
  const {
    defaultValue,
    value
  } = option;
  const [colorValue, setColorValue] = useState(() => {
    let mergeState;
    if (hasValue(value)) {
      mergeState = value;
    } else if (hasValue(defaultValue)) {
      mergeState = defaultValue;
    } else {
      mergeState = defaultStateValue;
    }
    return generateColor(mergeState || '');
  });
  useEffect(() => {
    if (value) {
      setColorValue(generateColor(value));
    }
  }, [value]);
  return [colorValue, setColorValue];
};
export default useColorState;