import React, { useState } from "react";
import DateTimeRangeContainer from "react-advanced-datetimerange-picker";
import moment from "moment";
import { CalendarIcon } from "@heroicons/react/outline";
import "./DateTimeRangePicker.css";

let now = new Date();
let start = moment(
  new Date(
    now.getFullYear(),
    now.getMonth(),
    now.getDate(),
    now.getHours(),
    now.getMinutes(),
    0,
    0,
  ),
);

let end = moment(start);
let rangeArr = [
  // "Live tracking",
  "Past 10 Minutes",
  "Past 1 Hour",
  "Past 5 Hours",
  "Past 24 Hours",
  "Past 3 Days",
  "Past 7 Days",
  "Past 2 Months",
];
let ranges = {
  // "Live tracking": [moment(start), moment(end)],
  "Past 10 Minutes": [moment(start).subtract(10, "minutes"), moment(end)],
  "Past 1 Hour": [moment(start).subtract(60, "minutes"), moment(end)],
  "Past 5 Hours": [moment(start).subtract(5, "hours"), moment(end)],
  "Past 24 Hours": [moment(start).subtract(24, "hours"), moment(end)],
  "Past 3 Days": [moment(start).subtract(3, "days"), moment(end)],
  "Past 7 Days": [moment(start).subtract(7, "days"), moment(end)],
  "Past 2 Months": [moment(start).subtract(2, "months"), moment(end)],
};
let local = {
  format: "DD-MM-YYYY HH:mm",
  sundayFirst: false,
};
let maxDate = moment(start);

const Picker = ({ setStartChange, setEndChange }) => {
  const [startDate, setStartDate] = useState(
    moment(start).subtract(10, "minutes"),
  );
  const [endDate, setEndDate] = useState(end);
  const [range, setRange] = useState(0);

  const applyCallback = (startDate, endDate) => {
    setStartDate(startDate);
    setEndDate(endDate);
    setStartChange(
      moment(startDate).utcOffset("+00:00").format("YYYY-MM-DDThh:mm:ssZ"),
    );
    setEndChange(
      moment(endDate).utcOffset("+00:00").format("YYYY-MM-DDThh:mm:ssZ"),
    );
  };

  const style = {
    fromDot: { backgroundColor: "rgb(100, 0, 34)" },
    toDot: { backgroundColor: "rgb(0, 135, 255)" },
    fromDate: {
      backgroundColor: "rgb(26 35 126)",
    },
    toDate: { backgroundColor: "rgb(26 35 126)" },
    betweenDates: {
      backgroundColor: "#90CAF9",
    },
    hoverCell: { color: "rgb(200, 0, 34)" },
    customRangeButtons: {
      backgroundColor: "white",
      border: "none",
      padding: "0.4rem 0.75rem",
      color: "rgb(66 66 66)",
      fontWeight: 500,
    },
    customRangeSelected: {
      backgroundColor: "rgb(26 35 126)",
      border: "none",
      padding: "0.4rem 0.75rem",
      fontWeight: 500,
    },
  };

  return (
    <div>
      <DateTimeRangeContainer
        ranges={ranges}
        start={startDate}
        end={endDate}
        local={local}
        maxDate={maxDate}
        style={style}
        smartMode={true}
        applyCallback={applyCallback}
        rangeCallback={(e) => setRange(e)}
        handleOutsideClick={(e) => console.log(e)}
      >
        <button className="search-button custom-focus mt-1 flex">
          <span className="block mr-auto">
            {range === 7 ? (
              <span className="text-sm">
                {moment(startDate).format("YY-MM-DD hh:mm")} -{" "}
                {moment(endDate).format("YY-MM-DD hh:mm")}
              </span>
            ) : (
              rangeArr[range]
            )}
          </span>
          <CalendarIcon
            className="h-6 w-6 text-textGrey block ml-auto"
            aria-hidden="true"
          />
        </button>
      </DateTimeRangeContainer>
    </div>
  );
};

export default Picker;
