export const LOCAL_SERVER_URL = "http://localhost:5678/";

export const getServerURL = () => {
  if (
    window.location.hostname === "localhost" ||
    window.location.hostname === "127.0.0.1"
  ) {
    return LOCAL_SERVER_URL;
  } else {
    return "/";
  }
};
