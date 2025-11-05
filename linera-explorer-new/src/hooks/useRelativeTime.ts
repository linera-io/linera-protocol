import { useState, useEffect, useMemo } from 'react';
import { formatRelativeTime } from '../utils/formatters';

/**
 * Custom hook to track relative time with automatic updates
 * @param timestamp - Timestamp in microseconds (Linera format)
 * @returns Human-readable relative time that updates every second
 */
export const useRelativeTime = (timestamp: number | null): string | null => {
  const [currentTime, setCurrentTime] = useState(new Date());

  // Update current time every second for real-time display
  useEffect(() => {
    const timer = setInterval(() => {
      setCurrentTime(new Date());
    }, 1000);

    return () => clearInterval(timer);
  }, []);

  // Calculate the relative time - recalculates when currentTime or timestamp changes
  const relativeTime = useMemo(() => {
    if (!timestamp) return null;

    const targetTime = new Date(timestamp / 1000);
    return formatRelativeTime(targetTime, currentTime);
  }, [currentTime, timestamp]);

  return relativeTime;
};
