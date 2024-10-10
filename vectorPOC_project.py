from flask import Flask, request, jsonify
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
import re

app = Flask(__name__)

# Example patient documents
patients = [
    {
        "name": "John",
        "age": 30,
        "diagnosis": "diabetes",
        "doctor": "Dr. Smith",
        "hospital": "City Hospital",
        "description": "John is 30 years old, diagnosed with diabetes. He is being treated by Dr. Smith at City Hospital."
    },
    {
        "name": "Alice",
        "age": 25,
        "diagnosis": "hypertension",
        "doctor": "Dr. Roberts",
        "hospital": "Mercy Clinic",
        "description": "Alice is 25 years old, suffering from hypertension. She is treated by Dr. Roberts at Mercy Clinic."
    },
    {
        "name": "Bob",
        "age": 50,
        "diagnosis": "arthritis",
        "doctor": "Dr. Lee",
        "hospital": "General Hospital",
        "description": "Bob is 50 years old, diagnosed with arthritis. His doctor is Dr. Lee at General Hospital."
    }
]

# Extract descriptions for vectorization
documents = [patient['description'] for patient in patients]

# Create the TfidfVectorizer
vectorizer = TfidfVectorizer(stop_words='english')
tfidf_matrix = vectorizer.fit_transform(documents)

# Function to perform basic entity recognition
def extract_entities(question):
    # Simple regex to find patient name, age, diagnosis, etc.
    name_match = re.search(r'\b(name|patient)\s*is\s*(\w+)', question, re.I)
    age_match = re.search(r'\b(age|old)\s*is\s*(\d+)', question, re.I)
    diagnosis_match = re.search(r'\b(diagnosis|diagnosed)\s*(with)?\s*(\w+)', question, re.I)
    
    return {
        "name": name_match.group(2) if name_match else None,
        "age": age_match.group(2) if age_match else None,
        "diagnosis": diagnosis_match.group(3) if diagnosis_match else None
    }

# Function to handle follow-up questions (simple context awareness)
previous_questions = {}

@app.route("/ask", methods=["POST"])
def ask_question():
    data = request.get_json()
    user_id = data.get("user_id", "anonymous")  # Use user ID to track context if needed
    question = data.get("question")

    # Check if it's a follow-up question
    if user_id in previous_questions:
        previous_context = previous_questions[user_id]
        if "follow-up" in previous_context:
            question = previous_context["follow-up"]

    # Extract entities from the question (simple entity recognition)
    entities = extract_entities(question)

    # If specific patient information is being requested
    if entities['name']:
        for patient in patients:
            if patient['name'].lower() == entities['name'].lower():
                return jsonify({
                    "answer": f"Patient {patient['name']} is {patient['age']} years old, diagnosed with {patient['diagnosis']}, treated by {patient['doctor']} at {patient['hospital']}."
                })

    # Convert the user question to vector
    query_vector = vectorizer.transform([question])

    # Calculate cosine similarities between the query and document vectors
    cosine_similarities = cosine_similarity(query_vector, tfidf_matrix).flatten()

    # Sort by similarity scores to handle multiple patients
    top_indices = cosine_similarities.argsort()[-3:][::-1]  # Top 3 closest matches

    # If multiple patients match, return multiple
    if cosine_similarities[top_indices[0]] == 0:
        return jsonify({"answer": "No relevant patient information found."})

    answers = []
    for idx in top_indices:
        answers.append(patients[idx]['description'])

    return jsonify({
        "answer": "Here are the closest matches: " + " | ".join(answers)
    })

# Function to handle follow-up questions by storing context
@app.route("/follow_up", methods=["POST"])
def follow_up():
    data = request.get_json()
    user_id = data.get("user_id", "anonymous")
    follow_up_question = data.get("follow_up_question")
    
    previous_questions[user_id] = {"follow-up": follow_up_question}
    return jsonify({"status": "Follow-up question received."})

if __name__ == "__main__":
    app.run(debug=True)


***************************************************************************
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Patient Diagnostics Chatbot</title>
    <style>
        /* Basic styling for the webpage */
        body {
            font-family: Arial, sans-serif;
            margin: 0;
            padding: 0;
            background-color: #f5f5f5;
        }

        .header {
            padding: 20px;
            text-align: center;
            background: #007bff;
            color: white;
        }

        .content {
            padding: 20px;
        }

        /* Chatbot button at the bottom-right corner */
        .chatbot-button {
            position: fixed;
            bottom: 30px;
            right: 30px;
            background-color: #007bff;
            color: white;
            border: none;
            border-radius: 50px;
            width: 60px;
            height: 60px;
            cursor: pointer;
            font-size: 24px;
        }

        /* Chat window */
        .chat-window {
            display: none;
            position: fixed;
            bottom: 100px;
            right: 30px;
            width: 300px;
            height: 400px;
            background-color: white;
            border: 1px solid #007bff;
            border-radius: 10px;
            box-shadow: 0px 4px 8px rgba(0, 0, 0, 0.1);
        }

        .chat-window header {
            background-color: #007bff;
            padding: 10px;
            color: white;
            font-size: 18px;
            text-align: center;
            border-radius: 10px 10px 0 0;
        }

        .chat-window .messages {
            padding: 10px;
            height: 300px;
            overflow-y: scroll;
        }

        .chat-window .input-box {
            display: flex;
            border-top: 1px solid #ccc;
        }

        .chat-window .input-box input {
            width: 80%;
            padding: 10px;
            border: none;
            outline: none;
        }

        .chat-window .input-box button {
            width: 20%;
            background-color: #007bff;
            color: white;
            border: none;
            cursor: pointer;
            padding: 10px;
        }

        /* Styling for messages */
        .message {
            margin: 5px 0;
        }

        .message.user {
            text-align: right;
        }

        .message.bot {
            text-align: left;
        }

    </style>
</head>
<body>

    <div class="header">
        <h1>Patient Diagnostics Dashboard</h1>
        <p>Use the chatbot to ask questions about patients' diagnostics.</p>
    </div>

    <div class="content">
        <h2>Welcome to the Patient Records Dashboard</h2>
        <p>Here, you can interact with our intelligent chatbot to ask questions related to patient records. Click on the chat icon at the bottom-right corner to begin your interaction.</p>
    </div>

    <!-- Chatbot Button -->
    <button class="chatbot-button" onclick="toggleChat()">💬</button>

    <!-- Chat Window -->
    <div class="chat-window" id="chat-window">
        <header>
            Patient Diagnostics Chatbot
        </header>
        <div class="messages" id="messages"></div>
        <div class="input-box">
            <input type="text" id="user-input" placeholder="Ask your question..." onkeypress="handleKeyPress(event)">
            <button onclick="sendMessage()">Send</button>
        </div>
    </div>

    <script>
        // Toggle chatbot window
        function toggleChat() {
            const chatWindow = document.getElementById('chat-window');
            chatWindow.style.display = chatWindow.style.display === 'none' || chatWindow.style.display === '' ? 'block' : 'none';
        }

        // Handle enter key press to send message
        function handleKeyPress(event) {
            if (event.key === 'Enter') {
                sendMessage();
            }
        }

        // Send user message to chatbot
        function sendMessage() {
            const userInput = document.getElementById('user-input').value;
            if (userInput.trim() === '') return;

            // Append user message to the chat
            appendMessage('user', userInput);

            // Clear input field
            document.getElementById('user-input').value = '';

            // Send user input to the backend
            fetch('http://127.0.0.1:5000/ask', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify({ question: userInput })
            })
            .then(response => response.json())
            .then(data => {
                // Append the bot response to the chat
                appendMessage('bot', data.answer);
            })
            .catch(error => {
                console.error('Error:', error);
                appendMessage('bot', 'Sorry, something went wrong.');
            });
        }

        // Append messages to chat window
        function appendMessage(sender, message) {
            const messagesDiv = document.getElementById('messages');
            const messageDiv = document.createElement('div');
            messageDiv.classList.add('message', sender);
            messageDiv.innerText = message;
            messagesDiv.appendChild(messageDiv);

            // Scroll chat to the bottom
            messagesDiv.scrollTop = messagesDiv.scrollHeight;
        }
    </script>

</body>
</html>
