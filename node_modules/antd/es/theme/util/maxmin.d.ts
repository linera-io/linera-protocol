export default function genMaxMin(type: 'css' | 'js'): {
    max: (...values: number[]) => number;
    min: (...values: number[]) => number;
} | {
    max: (...args: (string | number)[]) => string;
    min: (...args: (string | number)[]) => string;
};
