import type { CSSInterpolation } from './hooks/useStyleRegister';
declare class Keyframe {
    private name;
    style: CSSInterpolation;
    constructor(name: string, style: CSSInterpolation);
    getName(hashId?: string): string;
    _keyframe: boolean;
}
export default Keyframe;
