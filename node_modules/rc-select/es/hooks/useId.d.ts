/** Is client side and not jsdom */
export declare const isBrowserClient: boolean;
/** Get unique id for accessibility usage */
export declare function getUUID(): number | string;
export default function useId(id?: string): string;
