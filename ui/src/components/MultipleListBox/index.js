import { useState } from "react";
import { Listbox } from "@headlessui/react";

const people = [
  { id: 1, name: "Durward Reynolds" },
  { id: 2, name: "Kenton Towne" },
  { id: 3, name: "Therese Wunsch" },
  { id: 4, name: "Benedict Kessler" },
  { id: 5, name: "Katelyn Rohan" },
];

export default function MultipleListBox() {
  const [selectedPeople, setSelectedPeople] = useState([]);

  return (
    <Listbox value={selectedPeople} onChange={setSelectedPeople} multiple>
      <Listbox.Button className="flex relative w-1/4 cursor-default rounded-lg bg-white py-2 pl-3 pr-10 text-left shadow-md focus:outline-none focus-visible:border-indigo-500 focus-visible:ring-2 focus-visible:ring-white focus-visible:ring-opacity-75 focus-visible:ring-offset-2 focus-visible:ring-offset-orange-300 sm:text-sm">
        {selectedPeople.length > 0
          ? selectedPeople.map((person) => (
              <span className="block p-1 truncate ml-1 bg-slate-200 rounded-md">
                {person.name}
              </span>
            ))
          : "Select Tags"}
      </Listbox.Button>
      <Listbox.Options>
        {people.map((person) => (
          <Listbox.Option key={person.id} value={person}>
            {person.name}
          </Listbox.Option>
        ))}
      </Listbox.Options>
    </Listbox>
  );
}
