import * as React from 'react';
export default function useDebounce(value) {
  const [cacheValue, setCacheValue] = React.useState(value);
  React.useEffect(() => {
    const timeout = setTimeout(() => {
      setCacheValue(value);
    }, value.length ? 0 : 10);
    return () => {
      clearTimeout(timeout);
    };
  }, [value]);
  return cacheValue;
}