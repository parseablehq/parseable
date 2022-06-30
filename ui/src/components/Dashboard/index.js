import moment from "moment";
import { useEffect, useState, Fragment } from "react";
import Layout from "../Layout";
import SideDialog from "../SideDialog";
import { useNavigate } from "react-router-dom";
import { Dialog, Listbox, Menu, Transition } from "@headlessui/react";
import {
  CalendarIcon,
  ChartBarIcon,
  FolderIcon,
  HomeIcon,
  InboxIcon,
  MenuAlt2Icon,
  UsersIcon,
  XIcon,
} from "@heroicons/react/outline";
import { SearchIcon } from "@heroicons/react/solid";
import { Disclosure } from "@headlessui/react";
import { ChevronDownIcon } from "@heroicons/react/outline";
import { Combobox } from "@headlessui/react";
import { CheckIcon, SelectorIcon } from "@heroicons/react/solid";
import axios from "axios";
import StreamIcon from "../../assets/images/Icon awesome-stream (1).svg";
import UserIcon from "../../assets/images//Icon feather-user.svg";
import Logo from "../../assets/images/Group 295.svg";
import Tv from "../../assets/images/Icon material-live-tv.svg";
import BeatLoader from "react-spinners/BeatLoader";

const override = {
  display: "block",
  margin: "0 auto",
  borderColor: "red",
};

const logTimes = [
  { id: 1, name: "Live Tracking", value: 1 },
  { id: 2, name: "Past 10 Minutes", value: 10 },
  { id: 3, name: "Past 1 Hour", value: 60 },
  { id: 4, name: "Past 5 Hours", value: 300 },
  { id: 5, name: "Past 24 Hours", value: 1440 },
  { id: 5, name: "Past 3 Days", value: 4320 },
  { id: 5, name: "Past 7 Days", value: 10080 },
];

function classNames(...classes) {
  return classes.filter(Boolean).join(" ");
}

const Dashboard = () => {
  const navigate = useNavigate();
  const [open, setOpen] = useState(false);
  const [clickedRow, setClickedRow] = useState({});
  const [timeZone, setTimeZone] = useState("UTC");
  const [logStreams, setLogStreams] = useState([]);
  const [selectedLogStream, setSelectedLogStream] = useState("");
  const [loading, setLoading] = useState(false);
  const [tableLoading, setTableLoading] = useState(false);
  const [data, setData] = useState([]);

  const [selected, setSelected] = useState({});
  const [query, setQuery] = useState("");
  const [selectedLogTime, setSelectedLogTime] = useState(logTimes[1]);
  const [noData, setNoData] = useState(false);
  const [searchInput, setSearchInput] = useState("");
  const [startTime, setStartTime] = useState(
    moment().utcOffset("+00:00").format("YYYY-MM-DDThh:mm:ssZ")
  );
  const [endTime, setEndTime] = useState(
    moment()
      .utcOffset("+00:00")
      .subtract(10, "minutes")
      .format("YYYY-MM-DDThh:mm:ssZ")
  );
  const time = "2022-06-13T14:17:20.012644671Z";
  console.log(moment().utcOffset("+00:00").format("YYYY-MM-DDThh:mm:ssZ"));
  console.log(
    "CURRENT TIME MINUS 10 MINUTES",
    moment()
      .utcOffset("+00:00")
      .subtract(10, "minutes")
      .format("YYYY-MM-DDThh:mm:ssZ")
  );

  const timeZoneChange = (e) => {
    setTimeZone(e.target.value);
  };

  const currentUser = localStorage.getItem("username");
  const labels = sessionStorage.getItem("labels");

  useEffect(() => {
    // console.log(currentUser);

    if (!currentUser) {
      console.log("No user found");
      navigate("/");
    } else {
      console.log("User Found");
      var myHeaders = new Headers();
      myHeaders.append("Authorization", "Basic cGFyc2VhYmxlOnBhcnNlYWJsZQ==");
      myHeaders.append("Content-Type", "applfilterication/json");

      var requestOptions = {
        method: "GET",
        headers: myHeaders,
        redirect: "follow",
      };

      fetch("/api/v1/logstream", requestOptions)
        .then((response) => {
          setLoading(true);
          return response.json();
        })
        .then((result) => {
          console.log(result);

          if (result.length > 0) {
            setLogStreams(result);
            setSelected(result[0]);

            selectStreamHandler(result[0])

            setLoading(false);
          } else {
            setNoData(true);
          }
        })
        .catch((error) => {
          console.log("error", error);
        })
        .finally(() => setLoading(false));
    }

    
  }, [currentUser]);

  const filteredStreams =
    query === ""
      ? logStreams
      : logStreams.filter((stream) =>
          stream.name
            .toLowerCase()
            .replace(/\s+/g, "")
            .includes(query.toLowerCase().replace(/\s+/g, ""))
        );

  const timeChangeHandler = (e) => {
    console.log(e);

    if (e !== "calendar") {
      setSelectedLogTime(e);
      setEndTime(moment().utcOffset("+00:00").format("YYYY-MM-DDThh:mm:ssZ"));
      setStartTime(
        moment()
          .utcOffset("+00:00")
          .subtract(e.value, "minutes")
          .format("YYYY-MM-DDThh:mm:ssZ")
      );
    }
  };

  const selectStreamHandler = (e) => {
    console.log(e);
    setSelected(e);

    setTableLoading(true);

    var data;

    if (e.name === "teststream") {
      data = JSON.stringify({
        query: `select * from ${e.name}`,
        startTime: "2022-06-29T09:05:00+00:00",
        endTime: "2022-06-29T09:06:00+00:00",
      });
    } else if (e.name === "teststream2") {
      data = JSON.stringify({
        query: "select * from teststream2",
        startTime: "2022-06-29T10:36:00+00:00",
        endTime: "2022-06-29T10:37:00+00:00",
      });
    } else if (e.name === "teststream3") {
      data = JSON.stringify({
        query: "select * from teststream3",
        startTime: "2022-06-29T10:42:00+00:00",
        endTime: "2022-06-29T10:43:00+00:00",
      });
    }

    var config = {
      method: "post",
      url: "/api/v1/query",
      headers: {
        Authorization: "Basic cGFyc2VhYmxlOnBhcnNlYWJsZQ==",
        "Content-Type": "application/json",
      },
      data: data,
    };

    axios(config)
      .then(function (response) {
        setTableLoading(false);
        console.log(response.data);
        setData(response.data);
      })
      .catch(function (error) {
        console.log("error", error);
      })
      .finally(() => setTableLoading(false));
  };

  return (
    <>
      {loading ? (
        <div>loading</div>
      ) : (
        <Layout labels={data.length > 0 && data[0]?.labels}>
          <div className="">
            <div className="sticky top-0  flex-shrink-0 flex h-24 items-center sm:px-5 bg-white shadow">
              <div className="flex-1 px-4 flex justify-between">
                <div className="flex-1 flex">
                  <div>
                    <label
                      htmlFor="location"
                      className="block text-xs text-gray-700"
                    >
                      Stream
                    </label>
                    <Combobox
                      value={selected}
                      onChange={(e) => selectStreamHandler(e)}
                    >
                      <div className="relative mt-1">
                        <div className="relative w-full cursor-default overflow-hidden rounded-lg border border-gray-500 bg-white text-left focus:outline-none focus-visible:ring-2 focus-visible:ring-white focus-visible:ring-opacity-75 focus-visible:ring-offset-2  sm:text-sm">
                          <Combobox.Input
                            className="w-full border-none py-2 pl-3 pr-10 text-sm leading-5 text-gray-900 focus:ring-0"
                            displayValue={(stream) => stream.name}
                            onChange={(event) => setQuery(event.target.value)}
                          />
                          <Combobox.Button className="absolute inset-y-0 right-0 flex items-center pr-2">
                            <SelectorIcon
                              className="h-5 w-5 text-gray-400"
                              aria-hidden="true"
                            />
                          </Combobox.Button>
                        </div>
                        <Transition
                          as={Fragment}
                          leave="transition ease-in duration-100"
                          leaveFrom="opacity-100"
                          leaveTo="opacity-0"
                          afterLeave={() => setQuery("")}
                        >
                          <Combobox.Options className="absolute mt-1 max-h-60 w-full overflow-auto rounded-md bg-white py-1 text-base shadow-lg ring-1 ring-black ring-opacity-5 focus:outline-none sm:text-sm">
                            {filteredStreams.length === 0 && query !== "" ? (
                              <div className="relative cursor-default select-none py-2 px-4 text-gray-700">
                                Nothing found.
                              </div>
                            ) : (
                              filteredStreams
                                .sort(function (a, b) {
                                  if (a.name < b.name) {
                                    return -1;
                                  }
                                  if (a.name > b.name) {
                                    return 1;
                                  }
                                  return 0;
                                })
                                .map((stream, index) => (
                                  <Combobox.Option
                                    key={index}
                                    className={({ active }) =>
                                      `relative cursor-default select-none py-2 pl-10 pr-4 ${
                                        active
                                          ? "bg-bluePrimary text-white"
                                          : "text-gray-900"
                                      }`
                                    }
                                    value={stream}
                                  >
                                    {({ selected, active }) => (
                                      <>
                                        <span
                                          className={`block truncate ${
                                            selected
                                              ? "font-medium"
                                              : "font-normal"
                                          }`}
                                        >
                                          {stream.name}
                                        </span>
                                        {selected ? (
                                          <span
                                            className={`absolute inset-y-0 left-0 flex items-center pl-3 ${
                                              active
                                                ? "text-white"
                                                : "text-bluePrimary"
                                            }`}
                                          >
                                            <CheckIcon
                                              className="h-5 w-5"
                                              aria-hidden="true"
                                            />
                                          </span>
                                        ) : null}
                                      </>
                                    )}
                                  </Combobox.Option>
                                ))
                            )}
                          </Combobox.Options>
                        </Transition>
                      </div>
                    </Combobox>
                  </div>

                  <div className=" ml-10 hidden sm:flex flex-col h-full justify-around">
                    <div className="text-xs text-gray-700">Ingested Logs</div>
                    <div className="font-bold text-xl">2 GB</div>
                  </div>
                </div>
                <div className="ml-4 flex items-center md:ml-6">
                  <Listbox
                    value={selectedLogTime}
                    onChange={(e) => timeChangeHandler(e)}
                  >
                    {({ open }) => (
                      <>
                        {/* <Listbox.Label className="block text-sm font-medium text-gray-700">
                        Assigned to
                      </Listbox.Label> */}
                        <div className="mt-1 relative">
                          <Listbox.Button className="relative w-52 border-r-0 bg-white border border-gray-300  shadow-sm pl-3 pr-10 py-2 text-left cursor-default focus:outline-none focus:ring-0 focus:border-gray-300 sm:text-sm">
                            <span className="block truncate">
                              {selectedLogTime.name}
                            </span>
                            <span className="absolute inset-y-0 right-0 flex items-center pr-2 pointer-events-none">
                              <CalendarIcon
                                className="h-5 w-5 text-bluePrimary"
                                aria-hidden="true"
                              />
                            </span>
                          </Listbox.Button>

                          <Transition
                            show={open}
                            as={Fragment}
                            leave="transition ease-in duration-100"
                            leaveFrom="opacity-100"
                            leaveTo="opacity-0"
                          >
                            <Listbox.Options className="absolute z-10 mt-1 w-full bg-white shadow-lg max-h-60 rounded-md text-base ring-1 ring-black ring-opacity-5 overflow-auto focus:outline-none sm:text-sm">
                              {logTimes.map((time) => (
                                <Listbox.Option
                                  key={time.id}
                                  className={({ active }) =>
                                    classNames(
                                      active
                                        ? "text-white bg-bluePrimary"
                                        : "text-bluePrimary",
                                      "cursor-default border-y border-gray-100 select-none relative py-2 pl-8 pr-4"
                                    )
                                  }
                                  value={time}
                                >
                                  {({ selected, active }) => (
                                    <>
                                      <span
                                        className={classNames(
                                          selected
                                            ? "font-semibold"
                                            : "font-normal",
                                          "block truncate text-center"
                                        )}
                                      >
                                        {time.name === "Live Tracking" ? (
                                          <div className="flex items-center justify-center">
                                            <div>
                                              <img
                                                src={Tv}
                                                className="w-4 group-hover:fill-white mr-2"
                                              />
                                            </div>{" "}
                                            <div>{time.name}</div>{" "}
                                          </div>
                                        ) : (
                                          <div> {time.name}</div>
                                        )}
                                      </span>

                                      {/* {selected ? (
                                      <span
                                        className={classNames(
                                          active
                                            ? "text-white"
                                            : "text-bluePrimary",
                                          "absolute inset-y-0 left-0 flex items-center pl-1.5"
                                        )}
                                      >
                                        <CheckIcon
                                          className="h-5 w-5"
                                          aria-hidden="true"
                                        />
                                      </span>
                                    ) : null} */}
                                    </>
                                  )}
                                </Listbox.Option>
                              ))}
                              {/* <div value={"calendar"}>
                                <div className="flex items-center justify-center">
                                  <div
                                    className="datepicker relative form-floating mb-3 xl:w-96"
                                    data-mdb-toggle-button="false"
                                  >
                                    <input
                                      id="date_"
                                      type="text"
                                      className="form-control block w-full px-3 py-1.5 text-base text-center font-normal placeholder-white text-white bg-bluePrimary bg-clip-padding border border-solid border-gray-300  transition ease-in-out m-0 focus:text-gray-700 focus:bg-white focus:border-blue-600 focus:outline-none"
                                      placeholder="Select a date"
                                      data-mdb-toggle="datepicker"
                                      // value="2022-01-20"
                                      onFocus={(e) => {
                                        e.currentTarget.type = "date";
                                        e.currentTarget.focus();
                                      }}
                                    />
                                  </div>
                                </div>
                              </div> */}
                            </Listbox.Options>
                          </Transition>
                        </div>
                      </>
                    )}
                  </Listbox>
                  <div className="mt-1 relative  ">
                    <input
                      type="text"
                      name="search"
                      id="search"
                      value={searchInput}
                      onChange={(e) => setSearchInput(e.target.value)}
                      className=" focus:ring-0 outline-none focus:border-gray-300 block w-80 sm:text-sm border-gray-300"
                      placeholder="Search"
                    />
                    <div className="absolute inset-y-0 right-0 pr-3 flex items-center pointer-events-none">
                      <SearchIcon
                        className="h-5 w-5 text-bluePrimary"
                        aria-hidden="true"
                      />
                    </div>
                  </div>
                </div>
              </div>
            </div>

            <div className="overflow-x-auto">
              <div className="inline-block min-w-full align-middle">
                <div className="overflow-hidden shadow ring-1 ring-black ring-opacity-5 md:rounded-lg"></div>
                <table className="min-w-full divide-y divide-gray-300">
                  <thead className=" bg-gray-200">
                    <tr>
                      <th
                        scope="col"
                        className="py-2 flex items-center justify-between  space-x-2 pl-4 pr-3 text-left text-sm font-semibold text-gray-900 sm:pl-6"
                      >
                        <div>Time</div>

                        <select
                          id="time"
                          name="time"
                          className="mt-1 block pl-3 pr-10 py-1 text-base border-gray-300 focus:outline-none sm:text-sm rounded-md"
                          defaultValue={timeZone}
                          onChange={(e) => timeZoneChange(e)}
                        >
                          <option value="UTC">UTC</option>
                          <option value="GMT">GMT</option>
                          <option value="IST">IST</option>
                        </select>
                      </th>
                      {/* <th
                    scope="col"
                    className="px-3 py-3.5 text-left text-sm font-semibold text-gray-900"
                  >
                    Priority
  </th> */}

                      <th
                        scope="col"
                        className="px-3 py-3.5 w-full text-left text-sm font-semibold text-gray-900"
                      >
                        Log
                      </th>
                      <th
                        scope="col"
                        className="hidden lg:block px-3 py-3.5 text-left text-sm font-semibold text-gray-900"
                      >
                        Tags
                      </th>
                      <th
                        scope="col"
                        className="relative py-3.5 pl-3 pr-4 sm:pr-6"
                      >
                        <span className="sr-only">Edit</span>
                      </th>
                    </tr>
                  </thead>
                  {tableLoading ? (
                    <tbody>
                      <tr align={"center"}>
                        <td></td>
                        <td className=" flex py-3 justify-center">
                          <BeatLoader
                            color={"#1A237E"}
                            loading={tableLoading}
                            cssOverride={override}
                            size={10}
                          />
                          <td></td>
                        </td>
                      </tr>
                    </tbody>
                  ) : (
                    <tbody className="divide-y divide-gray-200 bg-white">
                      {data
                        ?.filter(
                          (data, index) => data.log.indexOf(searchInput) !== -1
                        )
                        .map((data, index) => (
                          <tr
                            onClick={() => {
                              setOpen(true);
                              setClickedRow(data);
                            }}
                            className="cursor-pointer hover:bg-slate-100 hover:shadow"
                            key={index}
                          >
                            <td className="whitespace-nowrap py-5 pl-4 pr-3 text-xs md:text-sm font-medium text-gray-900 sm:pl-6">
                              {timeZone === "UTC" || timeZone === "GMT"
                                ? moment
                                    .utc(data.time)
                                    .format("DD/MM/YYYY, HH:mm:ss")
                                : moment(data.time)
                                    .utcOffset("+05:30")
                                    .format("DD/MM/YYYY, HH:mm:ss")}
                            </td>
                            <td className="truncate text-ellipsis overflow-hidden max-w-200 sm:max-w-xs md:max-w-sm lg:max-w-sm  xl:max-w-md px-3 py-4 text-xs md:text-sm text-gray-700">
                              {data.log}
                            </td>
                            <td className="hidden xl:flex  whitespace-nowrap px-3 py-4 text-sm text-gray-700">
                              {data.labels
                                .split(",")
                                .filter((tag, index) => index <= 2)
                                .map((tag, index) => (
                                  <div className="mx-1  bg-slate-200 rounded-sm flex justify-center items-center px-1 py-1">
                                    {tag}
                                  </div>
                                ))}
                            </td>
                            <td className="hidden lg:flex xl:hidden whitespace-nowrap px-3 py-4 text-sm text-gray-700">
                              {data.labels
                                .split(",")
                                .filter((tag, index) => index <= 1)
                                .map((tag, index) => (
                                  <div className="mx-1  bg-slate-200 rounded-sm flex justify-center items-center px-1 py-1">
                                    {tag}
                                  </div>
                                ))}
                            </td>
                          </tr>
                        ))}
                    </tbody>
                  )}
                </table>
              </div>
            </div>
          </div>

          <SideDialog open={open} setOpen={setOpen} data={clickedRow} />
        </Layout>
      )}
    </>
  );
};

export default Dashboard;
