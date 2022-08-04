import { Fragment } from "react";
import { Dialog, Transition } from "@headlessui/react";
import { XIcon } from "@heroicons/react/outline";
import { Disclosure } from "@headlessui/react";
import { ChevronDownIcon } from "@heroicons/react/outline";
import Logo from "../../assets/images/Group 295.svg";

const Sidebar = ({ setSidebarOpen, sidebarOpen, labels }) => {
  function classNames(...classes) {
    return classes.filter(Boolean).join(" ");
  }

  const labelClickHandler = (label) => {
    const previousLabels = sessionStorage.getItem("labels");
    if (previousLabels !== null) {
      sessionStorage.setItem("labels", `${previousLabels},${label}`);
    } else {
      sessionStorage.setItem("labels", label);
    }
  };

  return (
    <>
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
                <Disclosure autoToggle as="div" className="pt-6 px-4">
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
                                "h-6 w-6 transform",
                              )}
                              aria-hidden="true"
                            />
                          </span>
                        </Disclosure.Button>
                      </dt>
                      <Disclosure.Panel as="dd" className="mt-2 pr-12">
                        <div className="flex mt-4 flex-col space-y-3 pb-4 pl-4 pr-10">
                          {labels
                            ? labels.split(",").map((label, index) => (
                                <div
                                  onClick={() => labelClickHandler(label)}
                                  key={index}
                                  className="cursor-pointer text-white font-light w-full border py-1  flex justify-center items-center border-white rounded-lg"
                                >
                                  {label}
                                </div>
                              ))
                            : null}
                        </div>
                      </Disclosure.Panel>
                    </>
                  )}
                </Disclosure>
              </Dialog.Panel>
            </Transition.Child>
            <div className="flex-shrink-0 w-14" aria-hidden="true"></div>
          </div>
        </Dialog>
      </Transition.Root>
    </>
  );
};

export default Sidebar;
