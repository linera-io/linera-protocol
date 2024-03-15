import type { Color } from '../color';
import type { ColorValueType } from '../interface';
declare const useColorState: (defaultStateValue: ColorValueType, option: {
    defaultValue?: ColorValueType;
    value?: ColorValueType;
}) => readonly [Color, React.Dispatch<React.SetStateAction<Color>>];
export default useColorState;
