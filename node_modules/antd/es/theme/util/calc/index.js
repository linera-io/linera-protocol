import NumCalculator from './NumCalculator';
import CSSCalculator from './CSSCalculator';
const genCalc = type => {
  const Calculator = type === 'css' ? CSSCalculator : NumCalculator;
  return num => new Calculator(num);
};
export default genCalc;