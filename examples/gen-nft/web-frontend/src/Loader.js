import React from 'react';

const Loader = () => {
    const spinnerStyle = {
        border: '4px solid #f3f3f3', // Light grey background
        borderTop: '4px solid #3498db', // Blue spinner
        borderRadius: '50%',
        width: '40px',
        height: '40px',
        animation: 'spin 2s linear infinite'
    };

    const keyframes = `
    @keyframes spin {
      0% { transform: rotate(0deg); }
      100% { transform: rotate(360deg); }
    }
  `;

    return (
        <>
            <style>{keyframes}</style>
            <div className="loader" style={spinnerStyle} aria-label="Loading"></div>
        </>
    );
};

export default Loader;