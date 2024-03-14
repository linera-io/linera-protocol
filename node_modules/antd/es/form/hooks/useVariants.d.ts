export declare const Variants: readonly ["outlined", "borderless", "filled"];
export type Variant = (typeof Variants)[number];
/**
 * Compatible for legacy `bordered` prop.
 */
declare const useVariant: (variant: Variant | undefined, legacyBordered?: boolean | undefined) => [Variant, boolean];
export default useVariant;
