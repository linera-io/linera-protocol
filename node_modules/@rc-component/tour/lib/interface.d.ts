import type { TriggerProps } from '@rc-component/trigger';
import type { PlacementType } from './placements';
import type { CSSProperties, ReactNode } from 'react';
import type { Gap } from './hooks/useTarget';
import { type DefaultPanelProps } from './TourStep/DefaultPanel';
export interface TourStepInfo {
    arrow?: boolean | {
        pointAtCenter: boolean;
    };
    target?: HTMLElement | (() => HTMLElement) | null | (() => null);
    title: ReactNode;
    description?: ReactNode;
    placement?: PlacementType;
    mask?: boolean | {
        style?: React.CSSProperties;
        color?: string;
    };
    className?: string;
    style?: CSSProperties;
    scrollIntoViewOptions?: boolean | ScrollIntoViewOptions;
    closeIcon?: ReactNode;
    closable?: boolean | ({
        closeIcon?: ReactNode;
    } & React.AriaAttributes);
}
export interface TourStepProps extends TourStepInfo {
    prefixCls?: string;
    total?: number;
    current?: number;
    onClose?: () => void;
    onFinish?: () => void;
    renderPanel?: (step: TourStepProps, current: number) => ReactNode;
    onPrev?: () => void;
    onNext?: () => void;
}
export interface TourProps extends Pick<TriggerProps, 'onPopupAlign'> {
    steps?: TourStepInfo[];
    open?: boolean;
    defaultCurrent?: number;
    current?: number;
    onChange?: (current: number) => void;
    onClose?: (current: number) => void;
    onFinish?: () => void;
    closeIcon?: TourStepProps['closeIcon'];
    closable?: TourStepProps['closable'];
    mask?: boolean | {
        style?: React.CSSProperties;
        color?: string;
    };
    arrow?: boolean | {
        pointAtCenter: boolean;
    };
    rootClassName?: string;
    placement?: PlacementType;
    prefixCls?: string;
    renderPanel?: (props: DefaultPanelProps, current: number) => ReactNode;
    gap?: Gap;
    animated?: boolean | {
        placeholder: boolean;
    };
    scrollIntoViewOptions?: boolean | ScrollIntoViewOptions;
    zIndex?: number;
    getPopupContainer?: TriggerProps['getPopupContainer'];
    builtinPlacements?: TriggerProps['builtinPlacements'] | ((config?: {
        arrowPointAtCenter?: boolean;
    }) => TriggerProps['builtinPlacements']);
    disabledInteraction?: boolean;
}
