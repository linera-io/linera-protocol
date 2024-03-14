import * as React from 'react';
import type { CSSMotionProps } from '../CSSMotion';
import type { MotionStatus, StepStatus } from '../interface';
export default function useStatus(supportMotion: boolean, visible: boolean, getElement: () => HTMLElement, { motionEnter, motionAppear, motionLeave, motionDeadline, motionLeaveImmediately, onAppearPrepare, onEnterPrepare, onLeavePrepare, onAppearStart, onEnterStart, onLeaveStart, onAppearActive, onEnterActive, onLeaveActive, onAppearEnd, onEnterEnd, onLeaveEnd, onVisibleChanged, }: CSSMotionProps): [MotionStatus, StepStatus, React.CSSProperties, boolean];
