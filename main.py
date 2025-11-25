import asyncio
from fastapi import FastAPI, Request, Response, WebSocket
from fastapi.responses import HTMLResponse
from fastapi.middleware.cors import CORSMiddleware

from data import people, todos
from implementations import CustomSubscriber

app = FastAPI()


app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],          # or specific origins
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["Access-Control-Allow-Origin"],
    

)


@app.post("/todos/{user_id}/{todo_title}")
async def root(request: Request, user_id: int, todo_title: str):
    from data import todos

    todos.insert({"userId": user_id, "title": todo_title, "completed": False})
    return {
        "message": "Todo added",
        "details": "if you have a running query, it will be updated it will automatically get updated",
    }


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    from data import my_todos

    data_to_send = {todo["id"]: todo for todo in my_todos.pull()}
    my_todos.subscribe(CustomSubscriber(on_add_callback=lambda todo: asyncio.create_task(websocket.send_json({"type": "add", "data": todo, "path": f".{todo['id']}"})),
     on_remove_callback=lambda todo: asyncio.create_task(websocket.send_json({"type": "remove", "data": todo, "path": f".{todo['id']}"})),
     on_update_callback=lambda todo: asyncio.create_task(websocket.send_json({"type": "update", "data": todo, "path": f".{todo['id']}"})),
     pull_callback=None))
    await websocket.send_json({"type": "load", "data": data_to_send})

    while True:
        data = await websocket.receive_text()
        await websocket.send_text(f"Message text was: {data}")


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
