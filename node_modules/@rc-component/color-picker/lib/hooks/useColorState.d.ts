import type { Color } from '../color';
import type { ColorGenInput } from '../interface';
type ColorValue = ColorGenInput | undefined;
declare const useColorState: (defaultStateValue: ColorValue, option: {
    defaultValue?: ColorValue;
    value?: ColorValue;
}) => [Color, React.Dispatch<React.SetStateAction<Color>>];
export default useColorState;
