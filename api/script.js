let params = new URLSearchParams(location.search);
console.log(window.location.href)

const userId = window.location.href.split('/').length === 4 ? window.location.href.split('/')[3] : null;

let reconnectAttempts = 0;

console.log(userId)

document.querySelector("#ws-id").textContent = userId;

const connectWebSocket = () => {
    const ws = new WebSocket(`ws://localhost:8000/ws/${userId}?authorization=abc123`);

    ws.onmessage = function(event) {
        var messages = document.getElementById('messages')
        var message = document.createElement('li')
        var content = document.createTextNode(event.data)
        message.appendChild(content)
        messages.appendChild(message)
        document.body.style.backgroundColor = "green"
        console.log(event.data);
    };

    //function sendMessage(event) {
    //    var input = document.getElementById("messageText")
    //    ws.send(input.value)
    //    input.value = ''
    //    event.preventDefault()
    //}

    ws.onopen = () => {
        reconnectAttempts = 0;
        const messages = document.getElementById('messages')
        messages.innerHTML = '';
    }

    ws.onclose = (event) => {
        console.warn("WebSocket closed:", event.reason);
        // Try reconnecting if the WebSocket wasn't closed manually
        let delay = 5000;
        reconnectAttempts++;
        console.log(`Reconnecting in ${delay / 1000} seconds...`);
        setTimeout(connectWebSocket, delay);
    };
};

connectWebSocket();
