import * as React from 'react';
export interface PerfRecord {
    renderWithProps: boolean;
}
declare const PerfContext: React.Context<PerfRecord>;
export default PerfContext;
