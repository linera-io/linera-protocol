import * as React from 'react';
import { pickProps } from "../../../utils/miscUtil";
var propNames = ['onMouseEnter', 'onMouseLeave'];
export default function useRootProps(props) {
  return React.useMemo(function () {
    return pickProps(props, propNames);
  }, [props]);
}