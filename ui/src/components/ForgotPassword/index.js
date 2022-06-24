import React from "react";
import { useNavigate } from "react-router-dom";
import logo from "../../assets/images/Group 308.svg";

const ForgotPassword = () => {
  const navigate = useNavigate();

  return (
    <div className="h-screen px-5 w-screen flex bg-login-back bg-cover md:bg-contain bg-top bg-no-repeat justify-center items-center">
      <div className="z-10 px-5 md:px-10 w-96 shadow-xl rounded-lg border bg-white border-gray-200 pt-8 pb-4 flex flex-col justify-center items-center">
        <img src={logo} className="w-32 px-4" />
        <div className="mt-6 text-bluePrimary font-bold text-sm">
          How to reset your password
        </div>
        <div className="mt-2 text-gray-700 text-sm">
          Follow the steps below to reset your password
        </div>

        <div className="mt-3 w-full">
          <div className="flex space-x-3">
            <div className="flex flex-col items-center">
              <div className="w-9 h-9 bg-bluePrimary rounded-full flex justify-center items-center text-white font-bold text-lg">
                1
              </div>
              <div className="w-px h-12 bg-gray-300"></div>
            </div>
            <div>
              <div className="mt-2 text-bluePrimary font-bold text-sm">
                Log into your console
              </div>
              <div className="mt-2 text-gray-700 text-xs">
                Small description of the step above to be added
              </div>
              <div className="mt-2 w-full h-px bg-gray-300"></div>
            </div>
          </div>
          <div className="flex space-x-3">
            <div className="flex flex-col items-center">
              <div className="w-9 h-9 bg-bluePrimary rounded-full flex justify-center items-center text-white font-bold text-lg">
                2
              </div>
              <div className="w-px h-12 bg-gray-300"></div>
            </div>
            <div>
              <div className="mt-2 text-bluePrimary font-bold text-sm">
                Update Password
              </div>
              <div className="mt-2 text-gray-700 text-xs">
                Small description of the step above to be added
              </div>
              <div className="mt-2 w-full h-px bg-gray-300"></div>
            </div>
          </div>
          <div className="flex space-x-3">
            <div className="flex flex-col items-center">
              <div className="w-9 h-9 bg-bluePrimary rounded-full flex justify-center items-center text-white font-bold text-lg">
                3
              </div>
            </div>
            <div>
              <div className="mt-2 text-bluePrimary font-bold text-sm">
                Reset the environment
              </div>
              <div className="mt-2 text-gray-700 text-xs">
                Small description of the step above to be added
              </div>
            </div>
          </div>
          <button
            onClick={() => navigate("/")}
            className="w-full py-3 flex justify-center items-center font-semibold text-yellowButton border border-yellowButton mt-7"
          >
            Login
          </button>
        </div>
      </div>
    </div>
  );
};

export default ForgotPassword;
