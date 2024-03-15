import * as React from 'react';
import type { MenuMode } from '../interface';
/**
 * Get focusable elements from the element set under provided container
 */
export declare function getFocusableElements(container: HTMLElement, elements: Set<HTMLElement>): HTMLElement[];
export declare const refreshElements: (keys: string[], id: string) => {
    elements: Set<HTMLElement>;
    key2element: Map<string, HTMLElement>;
    element2key: Map<HTMLElement, string>;
};
export declare function useAccessibility<T extends HTMLElement>(mode: MenuMode, activeKey: string, isRtl: boolean, id: string, containerRef: React.RefObject<HTMLUListElement>, getKeys: () => string[], getKeyPath: (key: string, includeOverflow?: boolean) => string[], triggerActiveKey: (key: string) => void, triggerAccessibilityOpen: (key: string, open?: boolean) => void, originOnKeyDown?: React.KeyboardEventHandler<T>): React.KeyboardEventHandler<T>;
