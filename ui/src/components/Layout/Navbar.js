import { Fragment, useState } from "react";
import { useNavigate } from "react-router-dom";
import Dialogue from "./Dialogue";
import { QuestionMarkCircleIcon } from "@heroicons/react/outline";
import { LogoutIcon, MenuAlt2Icon } from "@heroicons/react/outline";
import StreamIcon from "../../assets/images/Icon awesome-stream (1).svg";
import UserIcon from "../../assets/images/Icon feather-user.svg";
import Logo from "../../assets/images/Group 295.svg";

const Navbar = ({setSidebarOpen}) => {
  const navigate = useNavigate();

  const [isHelpDialogueOpen, setIsHelpDialogueOpen] = useState(false);

  return (
    <>
      <Dialogue isOpen={isHelpDialogueOpen} setIsOpen={setIsHelpDialogueOpen} />
      <div className=" sticky top-0 z-10 px-10 flex-shrink-0 flex h-16 bg-bluePrimary border-b-2 border-gray-500 shadow">
        <button
          type="button"
          className="px-4 border-r border-gray-200 text-gray-500 focus:outline-none focus:ring-2 focus:ring-inset focus:ring-bluePrimary md:hidden"
          onClick={() => setSidebarOpen(true)}
        >
          <span className="sr-only">Open sidebar</span>
          <MenuAlt2Icon className="h-6 w-6" aria-hidden="true" />
        </button>
        <div className="flex-1 px-4 flex justify-between">
          <div className="flex-1 flex">
            <img alt="" className="w-32" src={Logo} />
          </div>
          <div className="ml-4 flex items-center md:ml-6">
            <button className="flex space-x-4 text-white font-medium h-full border-b-4 pl-7 pr-8 items-center  justify-center  border-white">
              <img alt="" src={StreamIcon} className="w-6" />
              <p>Streams</p>
            </button>
            <button
              onClick={() => setIsHelpDialogueOpen(true)}
              className={
                "flex text-gray-400 py-5 px-7 text-sm border border-l-0 border-t-0 border-b-0 border-r-1 border-gray-400 custom-focus"
              }
            >
              <QuestionMarkCircleIcon className="h-5 w-5 my-auto mr-2" />
              <span className={"block mb-1"}>Help</span>
            </button>
            <div className="flex  mx-8">
              <img alt="" className="w-3" src={UserIcon} />
              <div className="ml-2 text-gray-400 text-sm">
                {localStorage.getItem("username")?.length > 0
                  ? localStorage.getItem("username")
                  : ""}
              </div>
            </div>
            <div>
              <LogoutIcon
                className="text-gray-400 w-5 ml-6"
                onClick={() => {
                  localStorage.removeItem("auth");
                  navigate("/");
                }}
              />
            </div>
          </div>
        </div>
      </div>
    </>
  );
};

export default Navbar;
