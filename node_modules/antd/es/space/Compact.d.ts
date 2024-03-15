import * as React from 'react';
import type { DirectionType } from '../config-provider';
import type { SizeType } from '../config-provider/SizeContext';
export interface SpaceCompactItemContextType {
    compactSize?: SizeType;
    compactDirection?: 'horizontal' | 'vertical';
    isFirstItem?: boolean;
    isLastItem?: boolean;
}
export declare const SpaceCompactItemContext: React.Context<SpaceCompactItemContextType | null>;
export declare const useCompactItemContext: (prefixCls: string, direction: DirectionType) => {
    compactSize: SizeType;
    compactDirection: "horizontal" | "vertical" | undefined;
    compactItemClassnames: string;
};
export declare const NoCompactStyle: React.FC<React.PropsWithChildren<{}>>;
export interface SpaceCompactProps extends React.HTMLAttributes<HTMLDivElement> {
    prefixCls?: string;
    size?: SizeType;
    direction?: 'horizontal' | 'vertical';
    block?: boolean;
    rootClassName?: string;
}
declare const Compact: React.FC<SpaceCompactProps>;
export default Compact;
