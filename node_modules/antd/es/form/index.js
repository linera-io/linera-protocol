"use client";

import warning from '../_util/warning';
import ErrorList from './ErrorList';
import InternalForm, { useForm, useWatch } from './Form';
import Item from './FormItem';
import List from './FormList';
import { FormProvider } from './context';
import useFormInstance from './hooks/useFormInstance';
const Form = InternalForm;
Form.Item = Item;
Form.List = List;
Form.ErrorList = ErrorList;
Form.useForm = useForm;
Form.useFormInstance = useFormInstance;
Form.useWatch = useWatch;
Form.Provider = FormProvider;
Form.create = () => {
  process.env.NODE_ENV !== "production" ? warning(false, 'Form', 'antd v4 removed `Form.create`. Please remove or use `@ant-design/compatible` instead.') : void 0;
};
export default Form;