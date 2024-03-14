import * as React from 'react';
export declare const IdContext: React.Context<string>;
export declare function getMenuId(uuid: string, eventKey: string): string;
/**
 * Get `data-menu-id`
 */
export declare function useMenuId(eventKey: string): string;
