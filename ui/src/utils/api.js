import axios from "axios";


export const getServerURL = () => {
  return "/";
};

const get = async (url) => {
  return await axios.get(getServerURL() + url, {
    headers: {
      Authorization: "Basic " + localStorage.getItem("auth"),
    },
  });
};

const post = async (url, data) => {
  return await axios.post(getServerURL() + url, data, {
    headers: {
      Authorization: "Basic " + localStorage.getItem("auth"),
    },
  });
};

export const getLogStream = async () => {
  return await get("api/v1/logstream");
};

export const queryLogs = async (streamName, startTime, endTime) => {
  return await post("api/v1/query", {
    query: `select * from ${streamName}`,
    startTime: startTime,
    endTime: endTime,
  });
};
