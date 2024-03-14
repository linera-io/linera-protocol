export type PrevSelectedIndex = null | number;
/**
 * @title multipleSelect hooks
 * @description multipleSelect by hold down shift key
 */
export default function useMultipleSelect<T, K>(getKey: (item: T) => K): readonly [(currentSelectedIndex: number, data: T[], selectedKeys: Set<K>) => K[], (val: PrevSelectedIndex) => void];
