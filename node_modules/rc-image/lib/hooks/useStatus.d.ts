type ImageStatus = 'normal' | 'error' | 'loading';
export default function useStatus({ src, isCustomPlaceholder, fallback, }: {
    src: string;
    isCustomPlaceholder?: boolean;
    fallback?: string;
}): readonly [(img?: HTMLImageElement) => void, {
    src: string;
    onLoad?: undefined;
} | {
    onLoad: () => void;
    src: string;
}, ImageStatus];
export {};
