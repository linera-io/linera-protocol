import getValue from 'rc-util/lib/utils/get';
import setValue from 'rc-util/lib/utils/set';
import type { InternalNamePath, NamePath, Store, EventArgs } from '../interface';
export { getValue, setValue };
/**
 * Convert name to internal supported format.
 * This function should keep since we still thinking if need support like `a.b.c` format.
 * 'a' => ['a']
 * 123 => [123]
 * ['a', 123] => ['a', 123]
 */
export declare function getNamePath(path: NamePath | null): InternalNamePath;
export declare function cloneByNamePathList(store: Store, namePathList: InternalNamePath[]): Store;
/**
 * Check if `namePathList` includes `namePath`.
 * @param namePathList A list of `InternalNamePath[]`
 * @param namePath Compare `InternalNamePath`
 * @param partialMatch True will make `[a, b]` match `[a, b, c]`
 */
export declare function containsNamePath(namePathList: InternalNamePath[], namePath: InternalNamePath, partialMatch?: boolean): boolean;
/**
 * Check if `namePath` is super set or equal of `subNamePath`.
 * @param namePath A list of `InternalNamePath[]`
 * @param subNamePath Compare `InternalNamePath`
 * @param partialMatch True will make `[a, b]` match `[a, b, c]`
 */
export declare function matchNamePath(namePath: InternalNamePath, subNamePath: InternalNamePath | null, partialMatch?: boolean): boolean;
type SimilarObject = string | number | object;
export declare function isSimilar(source: SimilarObject, target: SimilarObject): boolean;
export declare function defaultGetValueFromEvent(valuePropName: string, ...args: EventArgs): any;
/**
 * Moves an array item from one position in an array to another.
 *
 * Note: This is a pure function so a new array will be returned, instead
 * of altering the array argument.
 *
 * @param array         Array in which to move an item.         (required)
 * @param moveIndex     The index of the item to move.          (required)
 * @param toIndex       The index to move item at moveIndex to. (required)
 */
export declare function move<T>(array: T[], moveIndex: number, toIndex: number): T[];
