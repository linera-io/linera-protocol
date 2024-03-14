import { useMemo } from 'react';
import PickerButton from '../PickerButton';
export default function useComponents(components) {
  return useMemo(() => Object.assign({
    button: PickerButton
  }, components), [components]);
}