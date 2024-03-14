import * as React from 'react';
export interface SubMenuListProps extends React.HTMLAttributes<HTMLUListElement> {
    children?: React.ReactNode;
}
declare const SubMenuList: React.ForwardRefExoticComponent<SubMenuListProps & React.RefAttributes<HTMLUListElement>>;
export default SubMenuList;
