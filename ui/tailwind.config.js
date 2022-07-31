/** @type {import('tailwindcss').Config} */
module.exports = {
  content: ["./src/**/*.{js,jsx,ts,tsx}", "./components/**/*.{js,jsx,ts,tsx}"],
  theme: {
    extend: {
      colors: {
        grey: "#bababa",
        bluePrimary: "#1A237E",
        yellowButton: "#F29C38",
        codeBack: "#242424",
        drawerBlue: "#171F6F",
        textBlack: "#4a4a4a",
        iconGrey: "#9ca3af"
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
