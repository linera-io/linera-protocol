import type { StackConfig } from '../interface';
type StackParams = Exclude<StackConfig, boolean>;
type UseStack = (config?: StackConfig) => [boolean, StackParams];
declare const useStack: UseStack;
export default useStack;
