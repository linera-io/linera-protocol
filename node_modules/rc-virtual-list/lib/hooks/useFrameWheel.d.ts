interface FireFoxDOMMouseScrollEvent {
    detail: number;
    preventDefault: VoidFunction;
}
export default function useFrameWheel(inVirtual: boolean, isScrollAtTop: boolean, isScrollAtBottom: boolean, horizontalScroll: boolean, 
/***
 * Return `true` when you need to prevent default event
 */
onWheelDelta: (offset: number, horizontal?: boolean) => void): [(e: WheelEvent) => void, (e: FireFoxDOMMouseScrollEvent) => void];
export {};
