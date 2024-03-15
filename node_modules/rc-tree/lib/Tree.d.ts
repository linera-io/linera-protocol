import * as React from 'react';
import { NodeDragEventHandler, NodeDragEventParams, NodeMouseEventHandler, NodeMouseEventParams } from './contextTypes';
import DropIndicator from './DropIndicator';
import { BasicDataNode, DataNode, Direction, EventDataNode, FieldNames, FlattenNode, IconType, Key, KeyEntities, NodeInstance, SafeKey, ScrollTo } from './interface';
import { NodeListRef } from './NodeList';
export interface CheckInfo<TreeDataType extends BasicDataNode = DataNode> {
    event: 'check';
    node: EventDataNode<TreeDataType>;
    checked: boolean;
    nativeEvent: MouseEvent;
    checkedNodes: TreeDataType[];
    checkedNodesPositions?: {
        node: TreeDataType;
        pos: string;
    }[];
    halfCheckedKeys?: Key[];
}
export interface AllowDropOptions<TreeDataType extends BasicDataNode = DataNode> {
    dragNode: TreeDataType;
    dropNode: TreeDataType;
    dropPosition: -1 | 0 | 1;
}
export type AllowDrop<TreeDataType extends BasicDataNode = DataNode> = (options: AllowDropOptions<TreeDataType>) => boolean;
export type DraggableFn = (node: DataNode) => boolean;
export type DraggableConfig = {
    icon?: React.ReactNode | false;
    nodeDraggable?: DraggableFn;
};
export type ExpandAction = false | 'click' | 'doubleClick';
export interface TreeProps<TreeDataType extends BasicDataNode = DataNode> {
    prefixCls: string;
    className?: string;
    style?: React.CSSProperties;
    focusable?: boolean;
    activeKey?: Key | null;
    tabIndex?: number;
    children?: React.ReactNode;
    treeData?: TreeDataType[];
    fieldNames?: FieldNames;
    showLine?: boolean;
    showIcon?: boolean;
    icon?: IconType;
    selectable?: boolean;
    expandAction?: ExpandAction;
    disabled?: boolean;
    multiple?: boolean;
    checkable?: boolean | React.ReactNode;
    checkStrictly?: boolean;
    draggable?: DraggableFn | boolean | DraggableConfig;
    defaultExpandParent?: boolean;
    autoExpandParent?: boolean;
    defaultExpandAll?: boolean;
    defaultExpandedKeys?: Key[];
    expandedKeys?: Key[];
    defaultCheckedKeys?: Key[];
    checkedKeys?: Key[] | {
        checked: Key[];
        halfChecked: Key[];
    };
    defaultSelectedKeys?: Key[];
    selectedKeys?: Key[];
    allowDrop?: AllowDrop<TreeDataType>;
    titleRender?: (node: TreeDataType) => React.ReactNode;
    dropIndicatorRender?: (props: {
        dropPosition: -1 | 0 | 1;
        dropLevelOffset: number;
        indent: number;
        prefixCls: string;
        direction: Direction;
    }) => React.ReactNode;
    onFocus?: React.FocusEventHandler<HTMLDivElement>;
    onBlur?: React.FocusEventHandler<HTMLDivElement>;
    onKeyDown?: React.KeyboardEventHandler<HTMLDivElement>;
    onContextMenu?: React.MouseEventHandler<HTMLDivElement>;
    onClick?: NodeMouseEventHandler<TreeDataType>;
    onDoubleClick?: NodeMouseEventHandler<TreeDataType>;
    onScroll?: React.UIEventHandler<HTMLElement>;
    onExpand?: (expandedKeys: Key[], info: {
        node: EventDataNode<TreeDataType>;
        expanded: boolean;
        nativeEvent: MouseEvent;
    }) => void;
    onCheck?: (checked: {
        checked: Key[];
        halfChecked: Key[];
    } | Key[], info: CheckInfo<TreeDataType>) => void;
    onSelect?: (selectedKeys: Key[], info: {
        event: 'select';
        selected: boolean;
        node: EventDataNode<TreeDataType>;
        selectedNodes: TreeDataType[];
        nativeEvent: MouseEvent;
    }) => void;
    onLoad?: (loadedKeys: Key[], info: {
        event: 'load';
        node: EventDataNode<TreeDataType>;
    }) => void;
    loadData?: (treeNode: EventDataNode<TreeDataType>) => Promise<any>;
    loadedKeys?: Key[];
    onMouseEnter?: (info: NodeMouseEventParams<TreeDataType>) => void;
    onMouseLeave?: (info: NodeMouseEventParams<TreeDataType>) => void;
    onRightClick?: (info: {
        event: React.MouseEvent;
        node: EventDataNode<TreeDataType>;
    }) => void;
    onDragStart?: (info: NodeDragEventParams<TreeDataType>) => void;
    onDragEnter?: (info: NodeDragEventParams<TreeDataType> & {
        expandedKeys: Key[];
    }) => void;
    onDragOver?: (info: NodeDragEventParams<TreeDataType>) => void;
    onDragLeave?: (info: NodeDragEventParams<TreeDataType>) => void;
    onDragEnd?: (info: NodeDragEventParams<TreeDataType>) => void;
    onDrop?: (info: NodeDragEventParams<TreeDataType> & {
        dragNode: EventDataNode<TreeDataType>;
        dragNodesKeys: Key[];
        dropPosition: number;
        dropToGap: boolean;
    }) => void;
    /**
     * Used for `rc-tree-select` only.
     * Do not use in your production code directly since this will be refactor.
     */
    onActiveChange?: (key: Key) => void;
    filterTreeNode?: (treeNode: EventDataNode<TreeDataType>) => boolean;
    motion?: any;
    switcherIcon?: IconType;
    height?: number;
    itemHeight?: number;
    itemScrollOffset?: number;
    virtual?: boolean;
    direction?: Direction;
    rootClassName?: string;
    rootStyle?: React.CSSProperties;
}
interface TreeState<TreeDataType extends BasicDataNode = DataNode> {
    keyEntities: KeyEntities<TreeDataType>;
    indent: number | null;
    selectedKeys: Key[];
    checkedKeys: Key[];
    halfCheckedKeys: Key[];
    loadedKeys: Key[];
    loadingKeys: Key[];
    expandedKeys: Key[];
    draggingNodeKey: Key;
    dragChildrenKeys: Key[];
    dropPosition: -1 | 0 | 1 | null;
    dropLevelOffset: number | null;
    dropContainerKey: Key | null;
    dropTargetKey: Key | null;
    dropTargetPos: string | null;
    dropAllowed: boolean;
    dragOverNodeKey: Key | null;
    treeData: TreeDataType[];
    flattenNodes: FlattenNode<TreeDataType>[];
    focused: boolean;
    activeKey: Key | null;
    listChanging: boolean;
    prevProps: TreeProps;
    fieldNames: FieldNames;
}
declare class Tree<TreeDataType extends DataNode | BasicDataNode = DataNode> extends React.Component<TreeProps<TreeDataType>, TreeState<TreeDataType>> {
    static defaultProps: {
        prefixCls: string;
        showLine: boolean;
        showIcon: boolean;
        selectable: boolean;
        multiple: boolean;
        checkable: boolean;
        disabled: boolean;
        checkStrictly: boolean;
        draggable: boolean;
        defaultExpandParent: boolean;
        autoExpandParent: boolean;
        defaultExpandAll: boolean;
        defaultExpandedKeys: any[];
        defaultCheckedKeys: any[];
        defaultSelectedKeys: any[];
        dropIndicatorRender: typeof DropIndicator;
        allowDrop: () => boolean;
        expandAction: boolean;
    };
    static TreeNode: React.FC<import("./interface").TreeNodeProps<DataNode>>;
    destroyed: boolean;
    delayedDragEnterLogic: Record<SafeKey, number>;
    loadingRetryTimes: Record<SafeKey, number>;
    state: TreeState<TreeDataType>;
    dragStartMousePosition: any;
    dragNode: NodeInstance<TreeDataType>;
    currentMouseOverDroppableNodeKey: any;
    listRef: React.RefObject<NodeListRef>;
    componentDidMount(): void;
    componentDidUpdate(): void;
    onUpdated(): void;
    componentWillUnmount(): void;
    static getDerivedStateFromProps(props: TreeProps, prevState: TreeState): Partial<TreeState<DataNode>>;
    onNodeDragStart: NodeDragEventHandler<TreeDataType, HTMLDivElement>;
    /**
     * [Legacy] Select handler is smaller than node,
     * so that this will trigger when drag enter node or select handler.
     * This is a little tricky if customize css without padding.
     * Better for use mouse move event to refresh drag state.
     * But let's just keep it to avoid event trigger logic change.
     */
    onNodeDragEnter: (event: React.DragEvent<HTMLDivElement>, node: NodeInstance<TreeDataType>) => void;
    onNodeDragOver: (event: React.DragEvent<HTMLDivElement>, node: NodeInstance<TreeDataType>) => void;
    onNodeDragLeave: NodeDragEventHandler<TreeDataType>;
    onWindowDragEnd: (event: any) => void;
    onNodeDragEnd: NodeDragEventHandler<TreeDataType>;
    onNodeDrop: (event: React.DragEvent<HTMLDivElement>, node: any, outsideTree?: boolean) => void;
    resetDragState(): void;
    cleanDragState: () => void;
    triggerExpandActionExpand: NodeMouseEventHandler;
    onNodeClick: NodeMouseEventHandler<TreeDataType>;
    onNodeDoubleClick: NodeMouseEventHandler<TreeDataType>;
    onNodeSelect: NodeMouseEventHandler<TreeDataType>;
    onNodeCheck: (e: React.MouseEvent<HTMLSpanElement>, treeNode: EventDataNode<TreeDataType>, checked: boolean) => void;
    onNodeLoad: (treeNode: EventDataNode<TreeDataType>) => Promise<void>;
    onNodeMouseEnter: NodeMouseEventHandler<TreeDataType>;
    onNodeMouseLeave: NodeMouseEventHandler<TreeDataType>;
    onNodeContextMenu: NodeMouseEventHandler<TreeDataType>;
    onFocus: React.FocusEventHandler<HTMLDivElement>;
    onBlur: React.FocusEventHandler<HTMLDivElement>;
    getTreeNodeRequiredProps: () => {
        expandedKeys: React.Key[];
        selectedKeys: React.Key[];
        loadedKeys: React.Key[];
        loadingKeys: React.Key[];
        checkedKeys: React.Key[];
        halfCheckedKeys: React.Key[];
        dragOverNodeKey: React.Key;
        dropPosition: 0 | 1 | -1;
        keyEntities: KeyEntities<TreeDataType>;
    };
    /** Set uncontrolled `expandedKeys`. This will also auto update `flattenNodes`. */
    setExpandedKeys: (expandedKeys: Key[]) => void;
    onNodeExpand: (e: React.MouseEvent<HTMLDivElement>, treeNode: EventDataNode<TreeDataType>) => void;
    onListChangeStart: () => void;
    onListChangeEnd: () => void;
    onActiveChange: (newActiveKey: Key | null) => void;
    getActiveItem: () => FlattenNode<TreeDataType>;
    offsetActiveKey: (offset: number) => void;
    onKeyDown: React.KeyboardEventHandler<HTMLDivElement>;
    /**
     * Only update the value which is not in props
     */
    setUncontrolledState: (state: Partial<TreeState<TreeDataType>>, atomic?: boolean, forceState?: Partial<TreeState<TreeDataType>> | null) => void;
    scrollTo: ScrollTo;
    render(): React.JSX.Element;
}
export default Tree;
