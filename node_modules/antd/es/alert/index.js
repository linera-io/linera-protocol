"use client";

import InternalAlert from './Alert';
import ErrorBoundary from './ErrorBoundary';
const Alert = InternalAlert;
Alert.ErrorBoundary = ErrorBoundary;
export default Alert;