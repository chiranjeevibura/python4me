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
    name_match = re.search(r'\b(name|patient)\s*(?:is|named)?\s*(\w+)', question, re.I)
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
        body {
            font-family: Arial, sans-serif;
            margin: 0;
            padding: 0;
        }

        .chat-container {
            position: fixed;
            right: 20px;
            bottom: 20px;
            width: 350px;
            max-width: 100%;
            background-color: white;
            box-shadow: 0px 0px 15px rgba(0, 0, 0, 0.2);
            border-radius: 10px;
            overflow: hidden;
        }

        .chat-header {
            background-color: #007bff;
            color: white;
            padding: 15px;
            text-align: center;
            font-size: 18px;
            font-weight: bold;
        }

        .chat-messages {
            padding: 15px;
            height: 300px;
            overflow-y: auto;
            background-color: #f7f7f7;
        }

        .chat-input {
            display: flex;
            border-top: 1px solid #ddd;
        }

        .chat-input input {
            width: 100%;
            padding: 10px;
            border: none;
            outline: none;
            font-size: 16px;
        }

        .chat-input button {
            padding: 10px 20px;
            border: none;
            background-color: #007bff;
            color: white;
            font-size: 16px;
            cursor: pointer;
        }
    </style>
</head>
<body>

<div class="chat-container">
    <div class="chat-header">
        Patient Diagnostics Chatbot
    </div>
    <div class="chat-messages" id="chatMessages"></div>
    <div class="chat-input">
        <input type="text" id="chatInput" placeholder="Ask a question...">
        <button onclick="sendMessage()">Send</button>
    </div>
</div>

<script>
    const chatMessages = document.getElementById('chatMessages');
    const chatInput = document.getElementById('chatInput');

    function sendMessage() {
        const message = chatInput.value;
        if (message.trim() === '') return;

        appendMessage('You', message);
        chatInput.value = '';

        fetch('http://127.0.0.1:5000/ask', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
            },
            body: JSON.stringify({ question: message }),
        })
        .then(response => {
            if (!response.ok) {
                throw new Error('Network response was not ok');
            }
            return response.json();
        })
        .then(data => {
            appendMessage('Bot', data.answer);
        })
        .catch(error => {
            console.error('Error:', error);
            appendMessage('Bot', 'Sorry, something went wrong.');
        });
    }

    function appendMessage(sender, message) {
        const messageDiv = document.createElement('div');
        messageDiv.textContent = `${sender}: ${message}`;
        chatMessages.appendChild(messageDiv);
        chatMessages.scrollTop = chatMessages.scrollHeight;
    }
</script>

</body>
</html>

