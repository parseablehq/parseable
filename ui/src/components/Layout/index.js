import { useState } from "react";
import Navbar from "./Navbar";
import Sidebar from "./Sidebar";

export default function Layout({ children, labels }) {
  const [sidebarOpen, setSidebarOpen] = useState(false);

  return (
    <>
      <Navbar setSidebarOpen={setSidebarOpen} />
      <Sidebar
        sidebarOpen={sidebarOpen}
        setSidebarOpen={setSidebarOpen}
        labels={labels}
      />
      <div className="">
        <div className="flex flex-col flex-1">
          <main>
            <div className="">
              <div className="w-full">
                <div className="">{children}</div>
              </div>
            </div>
          </main>
        </div>
      </div>
    </>
  );
}
