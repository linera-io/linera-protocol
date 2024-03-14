import type { CSSObject } from '..';
export interface Transformer {
    visit?: (cssObj: CSSObject) => CSSObject;
}
