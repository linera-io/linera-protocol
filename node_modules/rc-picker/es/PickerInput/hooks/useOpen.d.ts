import type { OpenConfig } from '../../interface';
/**
 * Control the open state.
 * Will not close if activeElement is on the popup.
 */
export default function useOpen(open?: boolean, defaultOpen?: boolean, disabledList?: boolean[], onOpenChange?: (open: boolean) => void): [open: boolean, setOpen: (open: boolean, config?: OpenConfig) => void];
