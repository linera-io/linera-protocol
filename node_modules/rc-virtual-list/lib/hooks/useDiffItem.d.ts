import type { GetKey } from '../interface';
export default function useDiffItem<T>(data: T[], getKey: GetKey<T>, onDiff?: (diffIndex: number) => void): [T];
