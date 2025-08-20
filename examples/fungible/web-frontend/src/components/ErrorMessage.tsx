import React from "react";
import { IconError } from "../assets/icons/Error";

type ErrorMessageProps = {
  msg: string
}


function ErrorMessage({ msg }: ErrorMessageProps) {
  return (
    <div className="w-full text-sm py-2 px-4 rounded-md  text-red-500 flex items-center gap-x-2">
      <IconError />
      <p>{msg}</p>
    </div>
  );
}

export default ErrorMessage;
