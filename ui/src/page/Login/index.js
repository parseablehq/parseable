import { useNavigate } from "react-router-dom";
import logo from "../../assets/images/Group 308.svg";
import React, { useState } from "react";
import { getLogStream } from "../../utils/api";

const Login = () => {
  const navigate = useNavigate();

  const [username, setUsername] = useState("");
  const [password, setPassword] = useState("");
  const [error, setError] = useState(false);
  const [errorText, setErrorText] = useState("");
  const [loading, setLoading] = useState("");

  const validate = () => {
    if (username.trim() === "") {
      return false;
    } else if (password.trim() === "") {
      return false;
    } else {
      return true;
    }
  };

  const loginHandler = async (e) => {
    e.preventDefault();

    setLoading(true);
    setError(false);
    if (validate()) {
      const credentials = btoa(username + ":" + password);
      localStorage.setItem("auth", credentials);
      localStorage.setItem("username", username);
      try {
        await getLogStream();
        navigate("/index.html");
      } catch (e) {
        setError(true);
        setErrorText("Invalid credentials");
      }
    } else {
      setError(true);
      setErrorText("Invalid username or password");
    }
    setLoading(false);
  };

  return (
    <div className="h-screen px-5 w-screen flex bg-login-back bg-cover md:bg-contain bg-top bg-no-repeat justify-center items-center">
      <div className="z-10 px-10 w-96 shadow-xl rounded-lg border bg-white border-gray-200 pt-12 pb-4 flex flex-col justify-center items-center">
        <img alt={"parseable"} src={logo} className="w-64 px-4" />
        <div className="mt-6 text-bluePrimary font-bold text-sm">Welcome!</div>
        <div className="mt-2 text-gray-700 text-sm">
          Add your credentials to login
        </div>

        <div className="mt-3 w-full">
          <form onSubmit={(e) => loginHandler(e)}>
            <div className="mt-1 w-full">
              <input
                type="username"
                name="username"
                id="username"
                required
                className="shadow-sm border-2 px-3 py-3 focus:outline outline-bluePrimary block w-full sm:text-sm border-gray-300 rounded-sm"
                placeholder="Username"
                value={username}
                onChange={(e) => setUsername(e.target.value)}
              />
            </div>
            <div className="mt-4 w-full">
              <input
                type="password"
                name="username"
                id="username"
                required
                className="shadow-sm border-2 px-3 py-3 focus:outline outline-bluePrimary block w-full sm:text-sm border-gray-300 rounded-sm"
                placeholder="Password"
                value={password}
                onChange={(e) => setPassword(e.target.value)}
              />
            </div>
            <button
              type="submit"
              disabled={loading}
              className="hover:bg-yellow-500 disabled:bg-yellow-300 transform duration-200 hover:shadow w-full py-3 flex justify-center items-center font-semibold text-white bg-yellowButton mt-3"
            >
              Login
            </button>
            {error && (
              <p className="text-red-600 text-center mt-1">{errorText}</p>
            )}
          </form>
          <div
            onClick={() => navigate("/forgot-password")}
            className="cursor-pointer mt-3 text-bluePrimary text-center underline text-sm"
          >
            Forgot password?
          </div>
        </div>
      </div>
    </div>
  );
};

export default Login;
