import React from "react";

type radioProps = {
  checked: string;
  setChecked: Function;
};

const TYPE_ACCOUNT = [
  {
    label: "User",
    value: "User",
  },
  {
    label: "Application",
    value: "Application",
  },
];

function Radio({ checked, setChecked }: radioProps) {
  const handleRadioChange = (e: { target: { value: any } }) => {
    setChecked(e.target.value);
  };

  return (
    <div className="flex gap-x-4 gap-y-4">
      {TYPE_ACCOUNT.map((item) => (
        <div className="inline-flex items-center gap-x-2" key={item.value}>
          <label
            className="relative flex items-center cursor-pointer"
            htmlFor={item.value}
          >
            <input
              id={item.value}
              type="radio"
              name={item.label}
              value={item.value}
              checked={checked === item.value}
              onChange={handleRadioChange}
              className="peer h-5 w-5 cursor-pointer appearance-none rounded-full border border-slate-300 checked:border-red-400 transition-all"
            />
            <span className="absolute bg-red-600 w-3 h-3 rounded-full opacity-0 peer-checked:opacity-100 transition-opacity duration-200 top-1/2 left-1/2 transform -translate-x-1/2 -translate-y-1/2"></span>
          </label>
          <label className="cursor-pointer" htmlFor={item.value}>
            {item.label || ""}
          </label>
        </div>
      ))}
    </div>
  );
}

export default Radio;
