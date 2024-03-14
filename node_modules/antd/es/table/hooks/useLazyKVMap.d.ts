import type { GetRowKey, Key } from '../interface';
export default function useLazyKVMap<RecordType>(data: readonly RecordType[], childrenColumnName: string, getRowKey: GetRowKey<RecordType>): ((key: Key) => RecordType)[];
