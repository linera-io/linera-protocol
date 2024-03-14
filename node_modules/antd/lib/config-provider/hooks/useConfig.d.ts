declare function useConfig(): {
    componentDisabled: import("../DisabledContext").DisabledType;
    componentSize: import("../SizeContext").SizeType;
};
export default useConfig;
