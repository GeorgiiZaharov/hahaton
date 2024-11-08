const { GoogleGenerativeAI } = require("@google/generative-ai");
const path = require('path');
const express = require('express');
const session = require('express-session');
const app = express();
const genAI = new GoogleGenerativeAI("AIzaSyDLBgaKILxXEkfptkXRGGNWUo8kozPAnI4");

// Настройка сессии
app.use(session({
    secret: 'your_secret_key',
    resave: false,
    saveUninitialized: true,
    cookie: { secure: false }
}));

// обработка статики и джосна
app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

// Эндпоинт для получения ответа от GPT
app.post('/api/gptresponse', async (req, res) => {
    const { msg } = req.body;

    if (!msg) {
        return res.status(400).json({ error: "Сообщение не должно быть пустым" });
    }

    try {
        const model = await genAI.getGenerativeModel({ model: "gemini-1.5-flash" });
        const result = await model.generateContent([msg]);

        if (result && result.response && typeof result.response.text === "function") {
            const botResponse = result.response.text();
            res.json({ message: botResponse });
        } else {
            throw new Error("Неверный формат ответа от модели");
        }
    } catch (error) {
        console.error('Ошибка генерации ответа:', error);
        res.status(500).json({ error: "Ошибка генерации ответа от GPT" });
    }
});

// Запуск сервера
const PORT = process.env.PORT || 5500;
app.listen(PORT, () => {
    console.log(`Server is running on http://localhost:${PORT}`);
});
