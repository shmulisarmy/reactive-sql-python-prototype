import type { Component } from "solid-js";
import { createMutable } from "solid-js/store";

const ws = new WebSocket("ws://localhost:8000/ws");

interface Message {
  type: "add" | "remove" | "update" | "load";
  path: string;
  data: any;
}
ws.onmessage = (event) => {
  const message: Message = JSON.parse(event.data);
  console.log({ message });
  switch (message.type) {
    case "load":
      {
        update_entire_dataTree(message.data);
      }
      break;
    case "add":
      {
        const split_path = message.path.split(".");
        split_path.splice(0, 1);
        const end_of_path = split_path.pop();
        const parent = split_path.reduce((acc, key) => acc[key], dataTree);
        parent[end_of_path] = message.data;
      }
      break;
    case "remove":
      {
        const split_path = message.path.split(".");
        split_path.splice(0, 1);
        const end_of_path = split_path.pop();
        const parent = split_path.reduce((acc, key) => acc[key], dataTree);
        delete parent[end_of_path];
      }
      break;
    case "update":
      {
        const split_path = message.path.split(".");
        split_path.splice(0, 1);
        const end_of_path = split_path.pop();
        const parent = split_path.reduce((acc, key) => acc[key], dataTree);
        parent[end_of_path] = message.data;
      }
      break;
    default:
      throw new Error("Invalid message type");
  }
};

const dataTree = createMutable({});

function update_entire_dataTree(newData: any) {
  Object.assign(dataTree, newData);
}

const backend_url = "http://localhost:8000";
function add_todo(todo_title: string, user_id: string) {
  fetch(`${backend_url}/todos/${user_id}/${todo_title}`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
  });
}


function get_form_input_value(form_ref: HTMLFormElement | null, input_id: string) {
  for (let i = 0; i < form_ref?.elements.length; i++) {
    const element = form_ref?.elements[i];
    if (element.id === input_id) {
      return (element as HTMLInputElement).value;
    }
  }
  throw new Error("Input not found");
}

const App: Component = () => {
  let form_ref: HTMLFormElement | null = null;
  return (
    <>
    <p class="text-4xl text-green-700 text-center py-20">
      {JSON.stringify(dataTree)}
    </p>
    <form onSubmit={(e: SubmitEvent) => {
      e.preventDefault();
      console.log({form_ref})
      add_todo(get_form_input_value(form_ref, "todo-title"), get_form_input_value(form_ref, "user-id"));
    }} ref={form_ref}>
      <input type="text" placeholder="Todo Title" id="todo-title" />
      <input type="number" placeholder="User ID" id="user-id" />
      <button type="submit">Add Todo</button>
    </form>
    </>
  );
};

export default App;
