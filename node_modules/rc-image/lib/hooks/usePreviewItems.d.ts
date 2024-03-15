import type { InternalItem, RegisterImage } from '../interface';
import type { GroupConsumerProps } from '../PreviewGroup';
export type Items = Omit<InternalItem, 'canPreview'>[];
/**
 * Merge props provided `items` or context collected images
 */
export default function usePreviewItems(items?: GroupConsumerProps['items']): [items: Items, registerImage: RegisterImage];
