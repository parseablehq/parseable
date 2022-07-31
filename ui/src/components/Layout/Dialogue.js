import { Fragment } from "react";
import { Dialog, Transition } from "@headlessui/react";
import SlackIcon from "../../assets/images/slack_icon.png";
import GitHubIcon from "../../assets/images/github_icon.png";
import DocumentationIcon from "../../assets/images/documentation_icon.png";
import { XIcon } from "@heroicons/react/outline";

const HelpDialog = ({ isOpen, setIsOpen }) => {
  function closeModal() {
    setIsOpen(false);
  }
  return (
    <Transition appear show={isOpen} as={Fragment}>
      <Dialog as="div" className="relative z-10" onClose={closeModal}>
        <Transition.Child
          as={Fragment}
          enter="ease-out duration-300"
          enterFrom="opacity-0"
          enterTo="opacity-100"
          leave="ease-in duration-200"
          leaveFrom="opacity-100"
          leaveTo="opacity-0"
        >
          <div className="fixed inset-0 bg-black bg-opacity-25" />
        </Transition.Child>

        <div className="fixed inset-0 overflow-y-auto">
          <div className="flex min-h-full items-center justify-center p-4 text-center">
            <Transition.Child
              as={Fragment}
              enter="ease-out duration-300"
              enterFrom="opacity-0 scale-95"
              enterTo="opacity-100 scale-100"
              leave="ease-in duration-200"
              leaveFrom="opacity-100 scale-100"
              leaveTo="opacity-0 scale-95"
            >
              <Dialog.Panel className="relative w-full max-w-3xl transform overflow-hidden rounded-lg text-center bg-gray-100 p-6 align-middle shadow-xl transition-all">
                <button
                  onClick={() => closeModal()}
                  className="absolute  right-4 top-4"
                >
                  <XIcon className="h-6 text-iconGrey" />
                </button>

                <Dialog.Title
                  as="h3"
                  className="text-lg font-medium leading-6 text-gray-900"
                >
                  Need any help?
                </Dialog.Title>
                <div className="mt-2">
                  <p className="text-sm text-gray-500">
                    Here you can find useful resources and information.
                  </p>
                </div>

                <div className="mt-8 flex justify-between	">
                  <a
                    href="https://launchpass.com/parseable"
                    target="_blank"
                    rel="noreferrer"
                    style={{
                      boxShadow: "0px 2px 10px 3px rgba(0, 0, 0, 0.075)",
                    }}
                    className="w-56 py-12 px-14 rounded-lg custom-focus"
                  >
                    <img
                      src={SlackIcon}
                      className="w-24 mb-10 h-24 mx-auto"
                      alt=""
                    />
                    <span className="block font-medium leading-6 text-lg">
                      Slack
                    </span>
                    <span className="block text-sm text-gray-500">
                      Connect with us
                    </span>
                  </a>

                  <a
                    href="https://github.com/parseablehq/parseable"
                    target="_blank"
                    rel="noreferrer"
                    style={{
                      boxShadow: "0px 2px 10px 3px rgba(0, 0, 0, 0.075)",
                    }}
                    className="w-56 py-12 px-14 rounded-lg custom-focus"
                  >
                    <img
                      src={GitHubIcon}
                      className="w-24 mb-10 h-24 mx-auto"
                      alt=""
                    />
                    <span className="block font-medium leading-6 text-lg">
                      GitHub
                    </span>
                    <span className="block text-sm text-gray-500">
                      Find resources
                    </span>
                  </a>

                  <a
                    href="https://www.parseable.io/docs"
                    target="_blank"
                    rel="noreferrer"
                    style={{
                      boxShadow: "0px 2px 10px 3px rgba(0, 0, 0, 0.075)",
                    }}
                    className="w-56 py-12 px-14 rounded-lg custom-focus"
                  >
                    <img
                      src={DocumentationIcon}
                      className="w-24 mb-10 h-24 mx-auto"
                      alt=""
                    />
                    <span className="block font-medium leading-6 text-lg">
                      Documentation
                    </span>
                    <span className="block text-sm text-gray-500">
                      Learn more
                    </span>
                  </a>
                </div>
              </Dialog.Panel>
            </Transition.Child>
          </div>
        </div>
      </Dialog>
    </Transition>
  );
};

export default HelpDialog;
