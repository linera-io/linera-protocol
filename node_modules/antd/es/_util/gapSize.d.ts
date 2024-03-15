import type { SizeType } from '../config-provider/SizeContext';
export declare function isPresetSize(size?: SizeType | string | number): size is SizeType;
export declare function isValidGapNumber(size?: SizeType | string | number): size is number;
