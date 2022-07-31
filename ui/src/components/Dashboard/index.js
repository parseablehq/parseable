import moment from "moment";
import { useEffect, useState, Fragment } from "react";
import Layout from "../Layout";
import SideDialog from "../SideDialog";
// import AdvanceDateTimePicker from "../AdvanceDateTimePicker";
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
import { QuestionMarkCircleIcon } from "@heroicons/react/outline";
import { CheckIcon, XCircleIcon, SelectorIcon } from "@heroicons/react/solid";

import axios from "axios";
import StreamIcon from "../../assets/images/Icon awesome-stream (1).svg";
import UserIcon from "../../assets/images//Icon feather-user.svg";
import Logo from "../../assets/images/Group 295.svg";
import Tv from "../../assets/images/Icon material-live-tv.svg";
import BeatLoader from "react-spinners/BeatLoader";
import MultipleListBox from "../MultipleListBox";
import { getServerURL } from "../../utils/api";
import "./index.css";
import DatetimeRangePicker from "react-datetime-range-picker";

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
  const [searchOpen, setsearchOpen] = useState(false);
  const [selected, setSelected] = useState({});
  const [query, setQuery] = useState("");
  const [searchQuery, setSearchQuery] = useState("");
  const [selectedLogTime, setSelectedLogTime] = useState(logTimes[1]);
  const [noData, setNoData] = useState(false);
  const [labelSelected, setLabelSelected] = useState([]);
  const [searchInput, setSearchInput] = useState("");
  const [searchSelected, setSearchSelected] = useState({});
  const [startTime, setStartTime] = useState(
    moment().utcOffset("+00:00").format("YYYY-MM-DDThh:mm:ssZ"),
  );
  const [endTime, setEndTime] = useState(
    moment()
      .utcOffset("+00:00")
      .subtract(10, "minutes")
      .format("YYYY-MM-DDThh:mm:ssZ"),
  );
  const time = "2022-06-13T14:17:20.012644671Z";
  console.log(moment().utcOffset("+00:00").format("YYYY-MM-DDThh:mm:ssZ"));
  console.log(
    "CURRENT TIME MINUS 10 MINUTES",
    moment()
      .utcOffset("+00:00")
      .subtract(10, "minutes")
      .format("YYYY-MM-DDThh:mm:ssZ"),
  );

  // console.log(moment().timeZone())
  console.log(moment().zoneName());
  console.log(moment().zoneAbbr());
  console.log(Intl.DateTimeFormat().resolvedOptions().timeZone);

  // console.log(moment().tz(Intl.DateTimeFormat().resolvedOptions().timeZone));

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

      fetch(`${getServerURL()}api/v1/logstream`, requestOptions)
        .then((response) => {
          setLoading(true);
          return response.json();
        })
        .then((result) => {
          console.log(result);

          if (result.length > 0) {
            setLogStreams(result);
            result.sort(function (a, b) {
              if (a.name < b.name) {
                return -1;
              }
              if (a.name > b.name) {
                return 1;
              }
              return 0;
            });
            setSelected(result[0]);

            selectStreamHandler(result[0]);

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
            .includes(query.toLowerCase().replace(/\s+/g, "")),
        );

  const filteredSTreamStreams =
    searchQuery === ""
      ? data
      : data.filter((data) =>
          data.log
            .toLowerCase()
            .replace(/\s+/g, "")
            .includes(searchQuery.toLowerCase().replace(/\s+/g, "")),
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
          .format("YYYY-MM-DDThh:mm:ssZ"),
      );
    }
  };

  const selectStreamHandler = (e) => {
    console.log(e);
    setSelected(e);

    setLabelSelected([]);
    setSearchSelected({});
    setSelectedLogTime(logTimes[1]);

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
    } else if (e.name === "teststream5") {
      data = JSON.stringify({
        query: "select * from teststream5",
        startTime: "2022-07-12T08:37:00+00:00",
        endTime: "2022-07-12T08:38:00+00:00",
      });
    }

    var config = {
      method: "post",
      url: `${getServerURL()}api/v1/query`,
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

  const clearLabel = (label) => {
    console.log(label);

    console.log(labelSelected);

    const labelArray = labelSelected;
    // labelArray.filter((item) => item !== label);
    console.log(labelArray.filter((item) => item !== label));
    const filteredArray = [...labelArray.filter((item) => item !== label)];
    setLabelSelected(filteredArray);
  };

  return (
    <>
      {loading ? (
        <div>loading</div>
      ) : (
        <Layout labels={data.length > 0 && data[0]?.labels}>
          {/* <MultipleListBox /> */}
          <div className="bg-white shadow">
            <div className="sticky top-0 flex-shrink-0 flex h-24 items-center  ">
              <div className="flex-1 px-4 flex justify-">
                <div className="flex- flex">
                  <div>
                    <label
                      htmlFor="location"
                      className="block text-xs text-gray-700"
                    >
                      Stream
                    </label>
                    <Combobox
                      value={selected}
                      onChange={(e) => {
                        selectStreamHandler(e);
                      }}
                    >
                      <div className="relative mt-1">
                        <Combobox.Input
                          className="custom-input custom-focus"
                          displayValue={(stream) => stream.name}
                          onChange={(event) => setQuery(event.target.value)}
                        />
                        <Combobox.Button className="absolute inset-y-0 right-0 flex items-center pr-2">
                          <SelectorIcon
                            className="h-5 w-5 text-gray-400"
                            aria-hidden="true"
                          />
                        </Combobox.Button>
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
                </div>
                <div>
                  <label
                    htmlFor="location"
                    className="ml-4 md:ml-6 block text-xs text-gray-700"
                  >
                    Search
                  </label>
                  <div className="ml-4 flex items-center md:ml-6">
                    <Listbox
                      value={selectedLogTime}
                      onChange={(e) => timeChangeHandler(e)}
                    >
                      {({ open }) => (
                        <>
                          <div className="mt-1 relative">
                            <Listbox.Button className="search-button custom-focus">
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
                              <Listbox.Options className="absolute z-10 mt-1 pt-2 pb-2 w-full bg-white border focus:outline-0 rounded shadow">
                                {logTimes.map((time) => (
                                  <Listbox.Option
                                    key={time.id}
                                    className={({ active }) =>
                                      classNames(
                                        active
                                          ? "text-white bg-bluePrimary"
                                          : "text-textBlack",
                                        "cursor-default border-y border-gray-100 select-none relative py-1 font-medium text-sm ",
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
                                              : "font-norma",
                                            "block truncate my-1 text-center",
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
                                <div value={"calendar"}>
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
                                </div>
                              </Listbox.Options>
                            </Transition>
                          </div>
                        </>
                      )}
                    </Listbox>
                    <Combobox
                      value={searchSelected}
                      onChange={(e) => {
                        setSearchSelected(e);
                        setsearchOpen(true);
                      }}
                    >
                      <div className="relative mt-1">
                        <div className="relative cursor-default w-96">
                          <Combobox.Input
                            className="search-input custom-focus placeholder-iconGrey"
                            // displayValue={(data) => 'Search'}
                            placeholder="search"
                            onChange={(event) =>
                              setSearchQuery(event.target.value)
                            }
                          />
                          <Combobox.Button className="absolute inset-y-0 right-0 flex items-center pr-2">
                            <SearchIcon
                              className="h-5 w-5 text-iconGrey"
                              aria-hidden="true"
                            />
                          </Combobox.Button>
                        </div>
                        <Transition
                          as={Fragment}
                          leave="transition ease-in duration-100"
                          leaveFrom="opacity-100"
                          leaveTo="opacity-0"
                          afterLeave={() => setSearchQuery("")}
                        >
                          <Combobox.Options className="absolute mt-1 max-h-60 w-full overflow-auto rounded-md bg-white py-1 text-base shadow-lg ring-1 ring-black ring-opacity-5 focus:outline-none sm:text-sm">
                            {filteredSTreamStreams.length === 0 &&
                            searchQuery !== "" ? (
                              <div className="relative cursor-default select-none py-2 px-4 text-gray-700">
                                Nothing found.
                              </div>
                            ) : (
                              filteredSTreamStreams?.map &&
                              filteredSTreamStreams?.map((data, index) => (
                                <Combobox.Option
                                  key={index}
                                  className={({ active }) =>
                                    `relative cursor-default select-none py-2 pl-10 pr-4 ${
                                      active
                                        ? "bg-bluePrimary text-white"
                                        : "text-gray-900"
                                    }`
                                  }
                                  value={data}
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
                                        {data.log}
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
                </div>

                <div className="ml-3 flex-1">
                  <label
                    htmlFor="location"
                    className="block text-xs text-gray-700"
                  >
                    Tag filters
                  </label>
                  <Listbox
                    value={labelSelected}
                    onChange={setLabelSelected}
                    multiple
                  >
                    <div className="relative w-full mt-1">
                      <Listbox.Button className="custom-input text-left custom-focus">
                        {labelSelected.length > 0
                          ? labelSelected.map((label) => (
                              <span className="relative block w-min py-px pl-1 pr-6 truncate ml-1 bg-slate-200 rounded-md">
                                {label}
                                <XCircleIcon
                                  onClick={() => clearLabel(label)}
                                  className="hover:text-gray-600 transform duration-200 text-gray-700 w-4 absolute top-1 right-1"
                                />
                              </span>
                            ))
                          : "Select Tags"}
                        <span className="pointer-events-none absolute inset-y-0 right-0 flex items-center pr-2">
                          <ChevronDownIcon
                            className="h-5 w-5 text-gray-400"
                            aria-hidden="true"
                          />
                        </span>
                      </Listbox.Button>
                      <Transition
                        as={Fragment}
                        leave="transition ease-in duration-100"
                        leaveFrom="opacity-100"
                        leaveTo="opacity-0"
                      >
                        <Listbox.Options className="absolute mt-1 max-h-60 w-full overflow-auto grid grid-cols-2 bg-white py-1 text-base shadow-lg ring-1 ring-black ring-opacity-5 focus:outline-none sm:text-sm">
                          {Object.keys(data).length !== 0 ? (
                            data[0]?.labels
                              .split(",")
                              .map((person, personIdx) => (
                                <Listbox.Option
                                  key={personIdx}
                                  className={({ active }) =>
                                    `relative cursor-default select-none py-2 px-2 ${
                                      active
                                        ? "bg-bluePrimary text-white"
                                        : "text-gray-900"
                                    }`
                                  }
                                  value={person}
                                >
                                  {({ selected }) => (
                                    <>
                                      <span
                                        className={`flex items-center truncate ${
                                          selected
                                            ? "font-medium"
                                            : "font-normal"
                                        }`}
                                      >
                                        {selected ? (
                                          <div className="w-4 h-4 mr-1 flex items-center justify-center bg-white rounded-sm border-2 border-bluePrimary">
                                            <CheckIcon className="w-3 h-3 font-bold text-bluePrimary" />
                                          </div>
                                        ) : (
                                          <div className="w-4 h-4 mr-1 bg-white rounded-sm border-2 border-gray-400"></div>
                                        )}
                                        {person}
                                      </span>
                                    </>
                                  )}
                                </Listbox.Option>
                              ))
                          ) : (
                            <Listbox.Option>Nothing Found</Listbox.Option>
                          )}
                        </Listbox.Options>
                      </Transition>
                    </div>
                  </Listbox>
                </div>
              </div>
            </div>

            {/* <DatetimeRangePicker onChange={(e) => console.log('skhd',e)} /> */}

            {/* <div className="w-44">
              <AdvanceDateTimePicker />
            </div> */}

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
                          className="mt-1 block pl-3 pr-10 py-1 text-base  bg-gray-200 border-gray-300 focus:outline-none sm:text-sm rounded-md"
                          defaultValue={timeZone}
                          onChange={(e) => timeZoneChange(e)}
                        >
                          <option value="UTC">UTC</option>
                          {/* <option value="GMT">GMT</option> */}
                          <option value="IST">IST</option>
                        </select>
                      </th>
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
                      {data.map &&
                        data?.map((data, index) => (
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

          {Object.keys(searchSelected).length !== 0 && (
            <SideDialog
              open={searchOpen}
              setOpen={setsearchOpen}
              data={searchSelected}
            />
          )}

          <SideDialog open={open} setOpen={setOpen} data={clickedRow} />
        </Layout>
      )}
    </>
  );
};

export default Dashboard;
