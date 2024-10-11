from flask import Flask, jsonify, request
from pymongo import MongoClient
from sentence_transformers import SentenceTransformer
from sklearn.metrics.pairwise import cosine_similarity
import numpy as np

app = Flask(__name__)

# MongoDB setup (replace with your credentials)
client = MongoClient('mongodb://localhost:27017/')
db = client['dataportal']
collections = db['documents']  # Collection for storing documents
vectors = db['vector']  # Collection for storing document embeddings

# Load NLP model
model = SentenceTransformer('all-MiniLM-L6-v2')

# Fetch metadata for all collections
@app.route('/fetch_metadata', methods=['GET'])
def fetch_metadata():
    all_collections = db.list_collection_names()
    metadata = {}
    for collection in all_collections:
        doc_count = db[collection].count_documents({})
        metadata[collection] = {'document_count': doc_count}
    return jsonify(metadata), 200

# Fetch a specific document by UID
@app.route('/fetch_document_by_uid', methods=['GET'])
def fetch_document_by_uid():
    uid = request.args.get('uid')
    document = collections.find_one({'uid': uid})
    if document:
        return jsonify(document), 200
    else:
        return jsonify({'error': 'Document not found'}), 404

# Insert new documents and create vectors
@app.route('/add_documents', methods=['POST'])
def add_documents():
    documents = request.json.get('documents', [])
    if not documents:
        return jsonify({'error': 'No documents to add'}), 400
    
    # Insert documents and create embeddings
    for doc in documents:
        collections.insert_one(doc)
        doc_vector = model.encode(doc['content'])
        vectors.insert_one({'uid': doc['uid'], 'vector': doc_vector.tolist()})
    
    return jsonify({'message': 'Documents added and vectors generated'}), 201

# Similarity search using cosine similarity
@app.route('/query_similarity', methods=['POST'])
def query_similarity():
    query_text = request.json.get('query')
    query_vector = model.encode([query_text])[0]
    
    all_vectors = list(vectors.find({}))
    uids = [doc['uid'] for doc in all_vectors]
    vectors_data = np.array([doc['vector'] for doc in all_vectors])
    
    similarities = cosine_similarity([query_vector], vectors_data)[0]
    top_indices = np.argsort(similarities)[::-1][:5]  # Return top 5 similar documents
    
    top_uids = [uids[i] for i in top_indices]
    similar_docs = [collections.find_one({'uid': uid}) for uid in top_uids]
    
    return jsonify(similar_docs), 200

if __name__ == '__main__':
    app.run(debug=True)
-------
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Data Management Portal</title>
  <style>
    body {
      font-family: Arial, sans-serif;
    }
    .dashboard {
      display: flex;
      flex-direction: column;
      align-items: center;
      margin-top: 20px;
    }
    .collection-card {
      border: 1px solid #ddd;
      padding: 10px;
      margin: 10px;
      width: 300px;
    }
    .chatbox {
      position: fixed;
      bottom: 20px;
      right: 20px;
      width: 300px;
      border: 1px solid #ddd;
      background: white;
      padding: 10px;
    }
    .chatbox input {
      width: 80%;
    }
    .chatbox button {
      width: 18%;
    }
  </style>
</head>
<body>

<div class="dashboard">
  <h1>Data Management Portal</h1>
  <div id="collections"></div>
</div>

<div class="chatbox">
  <div id="chatLog"></div>
  <input type="text" id="userInput" placeholder="Ask something...">
  <button onclick="sendMessage()">Send</button>
</div>

<script>
  // Fetch collections metadata
  async function fetchCollections() {
    const response = await fetch('http://localhost:5000/fetch_metadata');
    const data = await response.json();
    const collectionsDiv = document.getElementById('collections');
    
    for (let collection in data) {
      const card = document.createElement('div');
      card.className = 'collection-card';
      card.innerHTML = `
        <h3>Collection: ${collection}</h3>
        <p>Document Count: ${data[collection].document_count}</p>
      `;
      collectionsDiv.appendChild(card);
    }
  }

  fetchCollections();

  // Chatbot functionality
  async function sendMessage() {
    const userMessage = document.getElementById('userInput').value;
    const chatLog = document.getElementById('chatLog');
    
    // Show user input in chat log
    const userChat = document.createElement('p');
    userChat.textContent = "You: " + userMessage;
    chatLog.appendChild(userChat);

    // Check if user is asking for UID document
    if (userMessage.startsWith("Show document")) {
      const uid = userMessage.split(" ")[2];
      const response = await fetch(`http://localhost:5000/fetch_document_by_uid?uid=${uid}`);
      const data = await response.json();
      
      const botResponse = document.createElement('p');
      botResponse.textContent = "Bot: " + JSON.stringify(data);
      chatLog.appendChild(botResponse);
    }
    // For any other queries (e.g., similarity search)
    else {
      const response = await fetch('http://localhost:5000/query_similarity', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json'
        },
        body: JSON.stringify({ query: userMessage })
      });
      const data = await response.json();
      
      const botResponse = document.createElement('p');
      botResponse.textContent = "Bot: " + JSON.stringify(data);
      chatLog.appendChild(botResponse);
    }

    document.getElementById('userInput').value = '';
  }
</script>

</body>
</html>
