export type ZIndexContainer = 'Modal' | 'Drawer' | 'Popover' | 'Popconfirm' | 'Tooltip' | 'Tour';
export type ZIndexConsumer = 'SelectLike' | 'Dropdown' | 'DatePicker' | 'Menu' | 'ImagePreview';
export declare const CONTAINER_MAX_OFFSET: number;
export declare const containerBaseZIndexOffset: Record<ZIndexContainer, number>;
export declare const consumerBaseZIndexOffset: Record<ZIndexConsumer, number>;
export declare function useZIndex(componentType: ZIndexContainer | ZIndexConsumer, customZIndex?: number): [zIndex: number | undefined, contextZIndex: number];
