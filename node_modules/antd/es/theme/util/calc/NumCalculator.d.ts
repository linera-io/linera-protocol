import AbstractCalculator from './calculator';
export default class NumCalculator extends AbstractCalculator {
    result: number;
    constructor(num: number | string | AbstractCalculator);
    add(num: number | string | AbstractCalculator): this;
    sub(num: number | string | AbstractCalculator): this;
    mul(num: number | string | AbstractCalculator): this;
    div(num: number | string | AbstractCalculator): this;
    equal(): number;
}
