import { type Ref } from 'react';
export default function useProxyImperativeHandle<NativeELementType extends HTMLElement, ReturnRefType extends {
    nativeElement: NativeELementType;
}>(ref: Ref<any> | undefined, init: () => ReturnRefType): void;
