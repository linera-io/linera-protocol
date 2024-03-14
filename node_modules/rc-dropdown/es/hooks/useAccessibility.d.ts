import * as React from "react";
interface UseAccessibilityProps {
    visible: boolean;
    triggerRef: React.RefObject<any>;
    onVisibleChange?: (visible: boolean) => void;
    autoFocus?: boolean;
    overlayRef?: React.RefObject<any>;
}
export default function useAccessibility({ visible, triggerRef, onVisibleChange, autoFocus, overlayRef, }: UseAccessibilityProps): void;
export {};
