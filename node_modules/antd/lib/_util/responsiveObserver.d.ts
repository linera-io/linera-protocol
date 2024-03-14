export type Breakpoint = 'xxl' | 'xl' | 'lg' | 'md' | 'sm' | 'xs';
export type BreakpointMap = Record<Breakpoint, string>;
export type ScreenMap = Partial<Record<Breakpoint, boolean>>;
export type ScreenSizeMap = Partial<Record<Breakpoint, number>>;
export declare const responsiveArray: Breakpoint[];
type SubscribeFunc = (screens: ScreenMap) => void;
export default function useResponsiveObserver(): {
    matchHandlers: {
        [prop: string]: {
            mql: MediaQueryList;
            listener: ((this: MediaQueryList, ev: MediaQueryListEvent) => any) | null;
        };
    };
    dispatch(pointMap: ScreenMap): boolean;
    subscribe(func: SubscribeFunc): number;
    unsubscribe(paramToken: number): void;
    unregister(): void;
    register(): void;
    responsiveMap: BreakpointMap;
};
export declare const matchScreen: (screens: ScreenMap, screenSizes?: ScreenSizeMap) => number | undefined;
export {};
