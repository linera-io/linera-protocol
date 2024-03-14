import { useContext } from 'react';
import { devUseWarning } from '../../_util/warning';
import { FormItemInputContext } from '../context';
const useFormItemStatus = () => {
  const {
    status,
    errors = [],
    warnings = []
  } = useContext(FormItemInputContext);
  if (process.env.NODE_ENV !== 'production') {
    const warning = devUseWarning('Form.Item');
    process.env.NODE_ENV !== "production" ? warning(status !== undefined, 'usage', 'Form.Item.useStatus should be used under Form.Item component. For more information: https://u.ant.design/form-item-usestatus') : void 0;
  }
  return {
    status,
    errors,
    warnings
  };
};
// Only used for compatible package. Not promise this will work on future version.
useFormItemStatus.Context = FormItemInputContext;
export default useFormItemStatus;