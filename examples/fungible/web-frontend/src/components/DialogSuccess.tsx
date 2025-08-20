import React from 'react';
import * as Dialog from '@radix-ui/react-dialog';
import {IconSuccess} from '../assets/icons/Success';

type DialogSuccessProps = {
  open: boolean;
  setOpenDialog: () => void;
}

export default function DialogSuccess({open, setOpenDialog}: DialogSuccessProps) {

  return (
    <Dialog.Root open={open} onOpenChange={setOpenDialog}>
      <Dialog.Portal>
        <Dialog.Overlay  className="fixed inset-0 z-50 bg-black/80  data-[state=open]:animate-in data-[state=closed]:animate-out data-[state=closed]:fade-out-0 data-[state=open]:fade-in-0"/>
        <Dialog.Content className="fixed left-[50%] top-[50%] z-50 grid w-full max-w-lg translate-x-[-50%] translate-y-[-50%] gap-4 border bg-white p-6 shadow-lg duration-200 data-[state=open]:animate-in data-[state=closed]:animate-out data-[state=closed]:fade-out-0 data-[state=open]:fade-in-0 data-[state=closed]:zoom-out-95 data-[state=open]:zoom-in-95 data-[state=closed]:slide-out-to-left-1/2 data-[state=closed]:slide-out-to-top-[48%] data-[state=open]:slide-in-from-left-1/2 data-[state=open]:slide-in-from-top-[48%] sm:rounded-lg">
            <div className="flex flex-col justify-center items-center">
                <IconSuccess />
                <p className="text-2xl md:text-3xl text-green-500 font-bold mt-4">Payment Successful!</p>
            </div>
            <Dialog.Close className="bg-green-500 rounded-md text-white text-lg py-2 font-bold mt-8">Close</Dialog.Close>
        </Dialog.Content>
      </Dialog.Portal>
    </Dialog.Root>
  );
};