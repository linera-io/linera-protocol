import type { Key, KeyEntities } from '../interface';
export default function getEntity<T = any>(keyEntities: KeyEntities<T>, key: Key): import("../interface").DataEntity<T>;
