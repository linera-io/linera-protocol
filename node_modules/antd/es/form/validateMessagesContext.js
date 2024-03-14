"use client";

import { createContext } from 'react';
// ZombieJ: We export single file here since
// ConfigProvider use this which will make loop deps
// to import whole `rc-field-form`
export default /*#__PURE__*/createContext(undefined);