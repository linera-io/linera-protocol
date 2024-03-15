import type { SizeType } from '../SizeContext';
declare const useSize: <T>(customSize?: T | ((ctxSize: SizeType) => T) | undefined) => T;
export default useSize;
