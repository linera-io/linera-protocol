import NumCalculator from './NumCalculator';
import CSSCalculator from './CSSCalculator';
import type AbstractCalculator from './calculator';
declare const genCalc: (type: 'css' | 'js') => (num: number | string | AbstractCalculator) => NumCalculator | CSSCalculator;
export default genCalc;
