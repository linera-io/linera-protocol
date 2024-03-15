import * as React from 'react';
export default function useSelection<T extends {
    key: string;
}>(leftDataSource: T[], rightDataSource: T[], selectedKeys?: string[]): [
    sourceSelectedKeys: string[],
    targetSelectedKeys: string[],
    setSourceSelectedKeys: React.Dispatch<React.SetStateAction<string[]>>,
    setTargetSelectedKeys: React.Dispatch<React.SetStateAction<string[]>>
];
