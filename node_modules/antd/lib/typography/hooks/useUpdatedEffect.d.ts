import * as React from 'react';
/** Similar with `useEffect` but only trigger after mounted */
declare const useUpdatedEffect: (callback: () => void, conditions?: React.DependencyList) => void;
export default useUpdatedEffect;
