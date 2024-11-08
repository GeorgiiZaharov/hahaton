document.getElementById("sendButton").addEventListener("click", sendMessage);
document.getElementById("messageInput").addEventListener("keydown", function (event) {
    if (event.key === "Enter") {
        sendMessage();
    }
});

function sendMessage() {
    const input = document.getElementById("messageInput");
    const message = input.value.trim();

    if (message === "") return;

    addMessage(message, "user");
    input.value = "";

    fetch('/api/gptresponse', {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json;charset=utf-8'
        },
        body: JSON.stringify({ msg: message })
    })
    .then(response => {
        if (!response.ok) {
            throw new Error("Сервер вернул ошибку");
        }
        return response.json();
    })
    .then(data => {
        if (data.message) {
            addMessage(data.message, "bot");
        } else {
            addMessage("Ошибка: Пустой ответ от сервера.", "bot");
        }
    })
    .catch(error => {
        console.error('Ошибка:', error);
        addMessage("Произошла ошибка при получении ответа от сервера. Проверьте подключение или обратитесь к разработчику.", "bot");
    });
}

function addMessage(text, sender) {
    const chatMessages = document.getElementById("chatMessages");
    const messageElement = document.createElement("div");
    messageElement.classList.add("chat-message", sender);
    messageElement.textContent = text;
    chatMessages.appendChild(messageElement);
    chatMessages.scrollTop = chatMessages.scrollHeight;
}
