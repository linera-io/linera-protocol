/// <reference types="react" />
import type { InputRef } from '../Input';
export default function useRemovePasswordTimeout(inputRef: React.RefObject<InputRef>, triggerOnMount?: boolean): () => void;
