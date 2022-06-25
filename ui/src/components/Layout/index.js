import { Fragment, useEffect, useState } from "react";
import { Dialog, Listbox, Menu, Transition } from "@headlessui/react";
import {
  CalendarIcon,
  ChartBarIcon,
  FolderIcon,
  HomeIcon,
  InboxIcon,
  LogoutIcon,
  MenuAlt2Icon,
  UsersIcon,
  XIcon,
} from "@heroicons/react/outline";
import { SearchIcon } from "@heroicons/react/solid";
import { Disclosure } from "@headlessui/react";
import { ChevronDownIcon } from "@heroicons/react/outline";
import { Combobox } from "@headlessui/react";
import { CheckIcon, SelectorIcon } from "@heroicons/react/solid";

import StreamIcon from "../../assets/images/Icon awesome-stream (1).svg";
import UserIcon from "../../assets/images//Icon feather-user.svg";
import Logo from "../../assets/images/Group 295.svg";
import Tv from "../../assets/images/Icon material-live-tv.svg";
import { useNavigate } from "react-router-dom";

const navigation = [
  { name: "Dashboard", href: "#", icon: HomeIcon, current: true },
  { name: "Team", href: "#", icon: UsersIcon, current: false },
  { name: "Projects", href: "#", icon: FolderIcon, current: false },
  { name: "Calendar", href: "#", icon: CalendarIcon, current: false },
  { name: "Documents", href: "#", icon: InboxIcon, current: false },
  { name: "Reports", href: "#", icon: ChartBarIcon, current: false },
];
const userNavigation = [
  { name: "Your Profile", href: "#" },
  { name: "Settings", href: "#" },
  { name: "Sign out", href: "#" },
];

function classNames(...classes) {
  return classes.filter(Boolean).join(" ");
}

const streams = [
  { id: 1, name: "Stream1" },
  { id: 2, name: "Stream 2" },
  { id: 3, name: "Stream 3" },
  { id: 4, name: "Stream 4" },
];

const logTimes = [
  { id: 1, name: "Live Tracking" },
  { id: 2, name: "Past 10 Minutes" },
  { id: 4, name: "Past 1 Hour" },
  { id: 4, name: "Past 5 Hour" },
  { id: 4, name: "Past 24 Hour" },
];

export default function Layout({ children }) {
  const navigate = useNavigate();
  const [sidebarOpen, setSidebarOpen] = useState(false);

  const [query, setQuery] = useState("");
  const [selectedLogTime, setSelectedLogTime] = useState(logTimes[0]);

  useEffect(() => {
    localStorage.getItem("username");
  }, []);

  const timeChangeHandler = (e) => {
    console.log(e);

    if (e !== "calendar") {
      setSelectedLogTime(e);
    }
  };

  return (
    <>
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
            <img className="w-32" src={Logo} />
          </div>
          <div className="ml-4 flex items-center md:ml-6">
            <button className="flex space-x-4 text-white font-medium h-full border-b-4 pl-7 pr-8 items-center  justify-center  border-white">
              {/* <MenuAlt1Icon className="w-6 ml-0" /> */}
              <img src={StreamIcon} className="w-6" />
              <p>Streams</p>
            </button>
            <div className="flex  ml-16">
              <img className="w-3" src={UserIcon} />
              <div className="ml-2 text-gray-400 text-sm">
                {localStorage.getItem("username").length > 0
                  ? localStorage.getItem("username")
                  : ""}
              </div>
            </div>
            <div>
              <LogoutIcon
                className="text-gray-400 w-5 ml-6"
                onClick={() => {
                  localStorage.removeItem("username");
                  navigate("/");
                }}
              />
            </div>
          </div>
        </div>
      </div>
      <div>
        <Transition.Root show={sidebarOpen} as={Fragment}>
          <Dialog
            as="div"
            className="relative z-40 md:hidden"
            onClose={setSidebarOpen}
          >
            <Transition.Child
              as={Fragment}
              enter="transition-opacity ease-linear duration-300"
              enterFrom="opacity-0"
              enterTo="opacity-100"
              leave="transition-opacity ease-linear duration-300"
              leaveFrom="opacity-100"
              leaveTo="opacity-0"
            >
              <div className="fixed inset-0 bg-gray-600 bg-opacity-75" />
            </Transition.Child>

            <div className="fixed inset-0 flex z-40">
              <Transition.Child
                as={Fragment}
                enter="transition ease-in-out duration-300 transform"
                enterFrom="-translate-x-full"
                enterTo="translate-x-0"
                leave="transition ease-in-out duration-300 transform"
                leaveFrom="translate-x-0"
                leaveTo="-translate-x-full"
              >
                <Dialog.Panel className="relative flex-1 flex flex-col max-w-xs w-full pt-5 pb-4 bg-bluePrimary">
                  <Transition.Child
                    as={Fragment}
                    enter="ease-in-out duration-300"
                    enterFrom="opacity-0"
                    enterTo="opacity-100"
                    leave="ease-in-out duration-300"
                    leaveFrom="opacity-100"
                    leaveTo="opacity-0"
                  >
                    <div className="absolute top-0 right-0 -mr-12 pt-2">
                      <button
                        type="button"
                        className="ml-1 flex items-center justify-center h-10 w-10 rounded-full focus:outline-none focus:ring-2 focus:ring-inset focus:ring-white"
                        onClick={() => setSidebarOpen(false)}
                      >
                        <span className="sr-only">Close sidebar</span>
                        <XIcon
                          className="h-6 w-6 text-white"
                          aria-hidden="true"
                        />
                      </button>
                    </div>
                  </Transition.Child>
                  <div className="flex-shrink-0 flex items-center px-4">
                    <img className="h-8 w-auto" src={Logo} alt="Workflow" />
                  </div>
                  <Disclosure as="div" className="pt-6 px-4">
                    {({ open }) => (
                      <>
                        <dt className="text-lg">
                          <Disclosure.Button className="text-left  w-full flex  justify-between items-start text-gray-400">
                            <div>
                              <div className="flex items-center flex-shrink-0 text-white text-lg font-semibold">
                                Tags Filters
                              </div>
                              <div className="text-white text-xs">
                                Select Filters
                              </div>
                            </div>
                            <span className="ml-6 h-7 flex items-center">
                              <ChevronDownIcon
                                className={classNames(
                                  open ? "-rotate-180" : "rotate-0",
                                  "h-6 w-6 transform"
                                )}
                                aria-hidden="true"
                              />
                            </span>
                          </Disclosure.Button>
                        </dt>
                        <Disclosure.Panel as="dd" className="mt-2 pr-12">
                          <div className="flex mt-4 flex-col space-y-3 pb-4 pl-4 pr-10">
                            <div className="text-white font-light w-full border py-1  flex justify-center items-center border-white rounded-lg">
                              Label 1
                            </div>
                            <div className="text-white font-light w-full border py-1  flex justify-center items-center border-white rounded-lg">
                              Label 2
                            </div>
                            <div className="text-white font-light w-full border py-1  flex justify-center items-center border-white rounded-lg">
                              Label 3
                            </div>
                            <div className="text-white font-light w-full border py-1  flex justify-center items-center border-white rounded-lg">
                              Label 4
                            </div>
                            <div className="text-white font-light w-full border py-1  flex justify-center items-center border-white rounded-lg">
                              Label 5
                            </div>
                            <div className="text-white font-light w-full border py-1  flex justify-center items-center border-white rounded-lg">
                              Label 6
                            </div>
                            <div className="text-white font-light w-full border py-1  flex justify-center items-center border-white rounded-lg">
                              Label 7
                            </div>
                          </div>
                        </Disclosure.Panel>
                      </>
                    )}
                  </Disclosure>

                  {/* <div className="flex items-center border-t border-gray-400 pt-4 px-4 text-white text-sm mt-4 space-x-2">
                    <img src={"/Icon feather-settings.svg"} className="w-5" />
                    <div>Settings</div>
                  </div> */}
                </Dialog.Panel>
              </Transition.Child>
              <div className="flex-shrink-0 w-14" aria-hidden="true">
                {/* Dummy element to force sidebar to shrink to fit close icon */}
              </div>
            </div>
          </Dialog>
        </Transition.Root>

        {/* Static sidebar for desktop */}
        <div className="hidden pt-16 md:flex md:w-64 md:flex-col md:fixed md:inset-y-0">
          {/* Sidebar component, swap this element with another sidebar if you like */}
          <div className="flex flex-col flex-grow  bg-bluePrimary overflow-y-auto">
            <div className=" flex-1 flex flex-col">
              <Disclosure as="div" className=" ">
                {({ open }) => (
                  <>
                    <dt className="text-lg">
                      <Disclosure.Button className="text-left transform duration-200 hover:bg-blue-800  py-3 px-4 w-full flex  justify-between items-start text-gray-400">
                        <div>
                          <div className="flex items-center flex-shrink-0 text-white text-lg font-semibold">
                            Tags
                          </div>
                          <div className="text-white text-xs">
                            Filter by tags
                          </div>
                        </div>
                        <span className="ml-6 h-7 flex items-center">
                          <ChevronDownIcon
                            className={classNames(
                              open ? "-rotate-180" : "rotate-0",
                              "h-6 w-6 transform"
                            )}
                            aria-hidden="true"
                          />
                        </span>
                      </Disclosure.Button>
                    </dt>
                    <Disclosure.Panel as="dd" className="mt-2 pr-12">
                      <div className="flex mt-4 flex-col space-y-3 pb-4 pl-4 pr-10">
                        <div className="text-white font-light w-full border py-1  flex justify-center items-center border-white rounded-lg">
                          Label 1
                        </div>
                        <div className="text-white font-light w-full border py-1  flex justify-center items-center border-white rounded-lg">
                          Label 2
                        </div>
                        <div className="text-white font-light w-full border py-1  flex justify-center items-center border-white rounded-lg">
                          Label 3
                        </div>
                        <div className="text-white font-light w-full border py-1  flex justify-center items-center border-white rounded-lg">
                          Label 4
                        </div>
                        <div className="text-white font-light w-full border py-1  flex justify-center items-center border-white rounded-lg">
                          Label 5
                        </div>
                        <div className="text-white font-light w-full border py-1  flex justify-center items-center border-white rounded-lg">
                          Label 6
                        </div>
                        <div className="text-white font-light w-full border py-1  flex justify-center items-center border-white rounded-lg">
                          Label 7
                        </div>
                      </div>
                    </Disclosure.Panel>
                  </>
                )}
              </Disclosure>

              {/* <div className="flex items-center border-t border-gray-400 pt-4 px-4 text-white text-sm m1 space-x-2">
                <img src={"/Icon feather-settings.svg"} className="w-5" />
                <div>Settings</div>
              </div> */}
            </div>
          </div>
        </div>
        <div className="md:pl-64 flex flex-col flex-1">
          <main>
            <div className="">
              {/* <div className="max-w-7xl mx-auto px-4 sm:px-6 md:px-8">
                <h1 className="text-2xl font-semibold text-gray-900">
                  Dashboard
                </h1>
              </div> */}
              <div className="max-w-7xl mx-auto">
                {/* Replace with your content */}
                <div className="">{children}</div>
                {/* /End replace */}
              </div>
            </div>
          </main>
        </div>
      </div>
    </>
  );
}
