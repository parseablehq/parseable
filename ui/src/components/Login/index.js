import { useNavigate } from "react-router-dom";
import logo from "../../assets/images/Group 308.svg";
import axios from "axios";
import React, { useState } from "react";

const Login = () => {
  const navigate = useNavigate();

  const [username, setUsername] = useState("");
  const [password, setPassword] = useState("");

  const validate = () => {
    if (username.trim() === "") {
      return false;
    } else if (password.trim() === "") {
      return false;
    } else {
      return true;
    }
  };

  const loginHandler = (e) => {
    e.preventDefault();

    if (validate()) {
      localStorage.setItem("username", username);
      localStorage.setItem("password", password);

      // var myHeaders = new Headers();
      // myHeaders.append("Authorization", `Basic ${btoa(username + password)}`);
      // myHeaders.append("Content-Type", "applfilterication/json");

      // var requestOptions = {
      //   method: "GET",
      //   headers: myHeaders,
      //   redirect: "follow",
      // };

      // fetch("http://localhost:5678/api/v1/logstream", requestOptions)
      //   .then((response) => console.log(response))
      //   .then((result) => {
      //     console.log(result);
      //     navigate("/index.html");
      //   })
      //   .catch((error) => console.log("error", error));
      navigate("/index.html");
    }
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
                className="shadow-sm border-2 italic px-3 py-3 focus:outline outline-bluePrimary block w-full sm:text-sm border-gray-300 rounded-sm"
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
                className="shadow-sm border-2 italic px-3 py-3 focus:outline outline-bluePrimary block w-full sm:text-sm border-gray-300 rounded-sm"
                placeholder="Password"
                value={password}
                onChange={(e) => setPassword(e.target.value)}
              />
            </div>
            <button
              type="submit"
              className="hover:bg-yellow-500 transform duration-200 hover:shadow w-full py-3 flex justify-center items-center font-semibold text-white bg-yellowButton mt-3"
            >
              Login
            </button>
          </form>
          <div
            onClick={() => navigate("/forgot-password")}
            className="cursor-pointer mt-3 text-bluePrimary text-center underline text-sm"
          >
            Forgot password?
          </div>
        </div>
      </div>

      {/* <div class="absolute bottom-0 w-screen overflow-hidden inline-block">
        <div class="h-screen w-screen bg-white rotate-45 transform origin-bottom-left"></div>
      </div> */}
    </div>
  );
};

export default Login;
