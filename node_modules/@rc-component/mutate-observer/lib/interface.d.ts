/// <reference types="react" />
export interface MutationObserverProps {
    children: React.ReactNode;
    options?: MutationObserverInit;
    onMutate?: (mutations: MutationRecord[], observer: MutationObserver) => void;
}
