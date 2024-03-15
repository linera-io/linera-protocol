/// <reference types="react" />
import type { Color } from '../color';
import type { TransformOffset } from '../interface';
type EventType = MouseEvent | React.MouseEvent<Element, MouseEvent> | React.TouchEvent<Element> | TouchEvent;
type EventHandle = (e: EventType) => void;
interface useColorDragProps {
    color?: Color;
    offset?: TransformOffset;
    containerRef: React.RefObject<HTMLDivElement>;
    targetRef: React.RefObject<HTMLDivElement>;
    direction?: 'x' | 'y';
    onDragChange?: (offset: TransformOffset) => void;
    onDragChangeComplete?: () => void;
    calculate?: (containerRef: React.RefObject<HTMLDivElement>) => TransformOffset;
    /** Disabled drag */
    disabledDrag?: boolean;
}
declare function useColorDrag(props: useColorDragProps): [TransformOffset, EventHandle];
export default useColorDrag;
