import asyncio
from http import client
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
import websockets
from fastapi.responses import HTMLResponse
import main


html = """
<!DOCTYPE html>
<html>
    <head>
        <title>Chat</title>
    </head>
    <body>
        <h1>Test Chat</h1>
        <h2>Your ID: <span id="ws-id"></span></h2>
        <form action="" onsubmit="sendMessage(event)">
            <input type="text" id="messageText" autocomplete="off"/>
            <button>Send</button>
        </form>
        <ul id='messages'>
        </ul>
        <script>
            let params = new URLSearchParams(location.search);
            const user_id = params.get('userID')
            document.querySelector("#ws-id").textContent = user_id;
            var ws = new WebSocket(`ws://localhost:8000/ws/${user_id}`);
            ws.onmessage = function(event) {
                var messages = document.getElementById('messages')
                var message = document.createElement('li')
                var content = document.createTextNode(event.data)
                message.appendChild(content)
                messages.appendChild(message)
            };
            function sendMessage(event) {
                var input = document.getElementById("messageText")
                ws.send(input.value)
                input.value = ''
                event.preventDefault()
            }
        </script>
    </body>
</html>
"""

App = FastAPI()

@app.get("/")
async def get():
    return HTMLResponse(html)

class WebsocketConnections:

    def __init__(self):
        self.activeConnections = [] #not used
        self.connectionsPreferences = {} #dictionary to map connection to preferences

    async def connect(self, websocket: WebSocket, client_id):
        await websocket.accept()
        self.activeConnections.append(websocket) #no longer needed
        self.connectionsPreferences[client_id] = websocket

    def disconnect(self, websocket: WebSocket):
        self.activeConnections.remove(websocket)

    async def broadcastWin(self, winningTeam):
        #for demonstration purposes
        for connections in self.activeConnections:
            await connections.send_text(winningTeam)
        """
        InterestedUsers = main.app.get("uid where followed_team == winning team") #temporary implementation, might work

        for user in InterestedUsers:
            if user in self.connectionsPreferences:
                await self.connectionsPreferences[user].send_text(winningTeam)
        """

    def updateAvailablePrefences(self, sports: list):
        for sport in sports:
            if sport not in self.connectionsPreferences:
                self.connectionsPreferences[sport] = []

manager = WebsocketConnections()

@app.websocket("/ws/{user_id}")
async def websocket_endpoint(websocket: WebSocket, user_id: int):
    await manager.connect(websocket, user_id)
    try:
        while True:
            data = await websocket.receive_text()
            await manager.broadcastWin(f"Client #{user_id} says: {data}")
    except WebSocketDisconnect:
        manager.disconnect(websocket)
        await manager.broadcastWin(f"Client #{user_id} left the chat")
