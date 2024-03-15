import * as React from 'react';
import type { InternalNamePath } from '../interface';
export default function useItemRef(): (name: InternalNamePath, children: any) => React.Ref<any> | undefined;
