import * as React from 'react';
import type { ItemType } from '../interface';
export declare function parseItems(children: React.ReactNode | undefined, items: ItemType[] | undefined, keyPath: string[]): React.ReactElement<any, string | React.JSXElementConstructor<any>>[];
