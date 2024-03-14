import { useEffect } from 'react';
import { devUseWarning } from '../../_util/warning';
const names = {};
export default function useFormWarning(_ref) {
  let {
    name
  } = _ref;
  const warning = devUseWarning('Form');
  useEffect(() => {
    if (name) {
      names[name] = (names[name] || 0) + 1;
      process.env.NODE_ENV !== "production" ? warning(names[name] <= 1, 'usage', 'There exist multiple Form with same `name`.') : void 0;
      return () => {
        names[name] -= 1;
      };
    }
  }, [name]);
}