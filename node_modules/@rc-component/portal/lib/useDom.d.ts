import type { QueueCreate } from './Context';
/**
 * Will add `div` to document. Nest call will keep order
 * @param render Render DOM in document
 */
export default function useDom(render: boolean, debug?: string): [HTMLDivElement, QueueCreate];
