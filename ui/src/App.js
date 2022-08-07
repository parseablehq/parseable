import "./App.css";
import { Routes, Route } from "react-router-dom";
import Login from "./page/Login";
import ForgotPassword from "./page/ForgotPassword";
import Dashboard from "./page/Dashboard";

function App() {
  return (
    <>
      <Routes>
        <Route path="/" element={<Login />} />
        <Route path="/forgot-password" element={<ForgotPassword />} />
        <Route path="/index.html" element={<Dashboard />} />
      </Routes>
    </>
  );
}

export default App;
