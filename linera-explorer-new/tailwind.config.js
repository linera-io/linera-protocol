/** @type {import('tailwindcss').Config} */
export default {
  content: [
    "./index.html",
    "./src/**/*.{js,ts,jsx,tsx}",
  ],
  theme: {
    extend: {
      colors: {
        'linera': {
          'dark': '#0a1f27',
          'darker': '#061116',
          'red': '#de2a02',
          'red-hover': '#c02402',
          'teal': '#1e4a52',
          'teal-light': '#2a5b65',
          'glow': '#4a90e2',
          'gray-light': '#8a9ba5',
          'gray-medium': '#6b7c87',
          'border': '#1a3339',
          'card': '#0f252c',
          'card-hover': '#143034',
        }
      },
      fontFamily: {
        'epilogue': ['Epilogue', 'sans-serif'],
        'inter': ['Inter', 'sans-serif'],
      },
      fontSize: {
        'body': ['18px', { lineHeight: '130%' }],
      },
      letterSpacing: {
        'tight-custom': '-0.02em',
      },
      backdropBlur: {
        'sm': '4px',
      },
      animation: {
        'fade-in': 'fadeIn 0.5s ease-in-out',
        'slide-up': 'slideUp 0.6s ease-out',
      },
      keyframes: {
        fadeIn: {
          '0%': { opacity: '0' },
          '100%': { opacity: '1' },
        },
        slideUp: {
          '0%': { transform: 'translateY(20px)', opacity: '0' },
          '100%': { transform: 'translateY(0)', opacity: '1' },
        },
      }
    },
  },
  plugins: [],
}