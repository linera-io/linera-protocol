/// <reference types="react" />
import { Key, FlattenNode } from '../interface';
export declare function findExpandedKeys(prev?: Key[], next?: Key[]): {
    add: boolean;
    key: import("react").Key;
};
export declare function getExpandRange(shorter: FlattenNode[], longer: FlattenNode[], key: Key): FlattenNode<import("../interface").DataNode>[];
