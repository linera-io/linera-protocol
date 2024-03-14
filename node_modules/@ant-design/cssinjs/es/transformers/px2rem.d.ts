import type { Transformer } from './interface';
interface Options {
    /**
     * The root font size.
     * @default 16
     */
    rootValue?: number;
    /**
     * The decimal numbers to allow the REM units to grow to.
     * @default 5
     */
    precision?: number;
    /**
     * Whether to allow px to be converted in media queries.
     * @default false
     */
    mediaQuery?: boolean;
}
declare const transform: (options?: Options) => Transformer;
export default transform;
