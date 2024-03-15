import { useContext } from 'react';
import { FormContext } from '../context';
export default function useFormInstance() {
  const {
    form
  } = useContext(FormContext);
  return form;
}