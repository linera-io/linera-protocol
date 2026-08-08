import ReactDOM from 'react-dom/client';
import * as linera from '@linera/client';

import { App } from './App';
import './styles.css';

await linera.initialize();

const root = document.getElementById('root');
if (!root) {
  throw new Error('missing #root element');
}

ReactDOM.createRoot(root).render(<App />);
