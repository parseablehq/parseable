/** @type {import('tailwindcss').Config} */
module.exports = {
  content: ["./src/**/*.{js,jsx,ts,tsx}", "./components/**/*.{js,jsx,ts,tsx}"],
  theme: {
    extend: {
      colors: {
        bluePrimary: "#1A237E",
        yellowButton: "#F29C38",
        codeBack: "#242424",
        drawerBlue: "#171F6F",
      },
      backgroundImage: {
        "login-back": "url('assets/images/Path 369.svg')",
      },
    },
  },
  plugins: [require("@tailwindcss/forms"), require("tailwind-scrollbar")],
  variants: {
    scrollbar: ["rounded"],
  },
};
