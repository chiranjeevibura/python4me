from flask import Flask, jsonify, request
from pymongo import MongoClient
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
import numpy as np

app = Flask(__name__)

# MongoDB setup (replace with your credentials)
client = MongoClient('mongodb://localhost:27017/')
db = client['dataportal']
collections = db['documents']  # Collection for storing documents
vectors = db['vector']  # Collection for storing document TF-IDF vectors

# Initialize TF-IDF Vectorizer (adjust max_features if needed)
vectorizer = TfidfVectorizer()

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

# Insert new documents and create TF-IDF vectors
@app.route('/add_documents', methods=['POST'])
def add_documents():
    documents = request.json.get('documents', [])
    if not documents:
        return jsonify({'error': 'No documents to add'}), 400

    # Insert documents and create TF-IDF vectors
    document_texts = [doc['content'] for doc in documents]
    
    # Store documents in the collection
    for doc in documents:
        collections.insert_one(doc)
    
    # Generate TF-IDF vectors for the documents
    tfidf_matrix = vectorizer.fit_transform(document_texts)
    
    # Store TF-IDF vectors in the "vectors" collection
    for i, doc in enumerate(documents):
        vectors.insert_one({'uid': doc['uid'], 'vector': tfidf_matrix[i].toarray().tolist()[0]})
    
    return jsonify({'message': 'Documents added and TF-IDF vectors generated'}), 201

# Similarity search using cosine similarity
@app.route('/query_similarity', methods=['POST'])
def query_similarity():
    query_text = request.json.get('query')
    
    # Transform the query into a TF-IDF vector
    query_vector = vectorizer.transform([query_text]).toarray()[0]
    
    # Fetch all stored document vectors from MongoDB
    all_vectors = list(vectors.find({}))
    uids = [doc['uid'] for doc in all_vectors]
    vectors_data = np.array([doc['vector'] for doc in all_vectors])
    
    # Calculate cosine similarity between the query and document vectors
    similarities = cosine_similarity([query_vector], vectors_data)[0]
    top_indices = np.argsort(similarities)[::-1][:5]  # Return top 5 similar documents
    
    # Fetch and return the top 5 most similar documents
    top_uids = [uids[i] for i in top_indices]
    similar_docs = [collections.find_one({'uid': uid}) for uid in top_uids]
    
    return jsonify(similar_docs), 200

if __name__ == '__main__':
    app.run(debug=True)


---------------- V2------------------


<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Ask MAA</title>
    <style>
        /* Chat window styling */
        .chat-container {
            position: fixed;
            bottom: 0;
            right: 15px;
            width: 300px;
            background-color: #f1f1f1;
            border: 1px solid #ccc;
            padding: 10px;
            border-radius: 10px;
        }

        .chat-box {
            width: 100%;
            height: 250px;
            border: 1px solid #ccc;
            padding: 10px;
            overflow-y: auto;
            background-color: white;
        }

        .chat-input {
            width: 100%;
            padding: 10px;
            margin-top: 10px;
        }

        .chat-submit {
            width: 100%;
            padding: 10px;
            background-color: #4CAF50;
            color: white;
            border: none;
            border-radius: 5px;
            cursor: pointer;
        }

        .chat-submit:hover {
            background-color: #45a049;
        }
    </style>
</head>
<body>

<div class="chat-container">
    <div class="chat-box" id="chat-box"></div>
    <input type="text" id="chat-input" class="chat-input" placeholder="Ask MAA...">
    <button id="chat-submit" class="chat-submit">Send</button>
</div>

<script>
    document.getElementById('chat-submit').onclick = async function() {
        let input = document.getElementById('chat-input').value;
        let chatBox = document.getElementById('chat-box');

        // Append user query to chat box
        chatBox.innerHTML += "<p><strong>You:</strong> " + input + "</p>";

        // Send the query to the Flask backend (make sure to use the correct backend URL)
        let response = await fetch('http://localhost:5000/chat', {  // Replace with your server URL if needed
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({ query: input })
        });

        let result = await response.json();

        // Append response to chat box
        chatBox.innerHTML += "<p><strong>MAA:</strong> " + result.response + "</p>";

        // Check if the result suggests asking for metadata
        if (result.response.includes("metadata")) {
            // If metadata is requested, send another query for metadata
            let uid = input.split('::')[1].trim();
            let metadataResponse = await fetch('http://localhost:5000/metadata', {  // Use full backend URL
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify({ uid: uid })
            });

            let metadata = await metadataResponse.json();
            chatBox.innerHTML += "<p><strong>Metadata:</strong> " + JSON.stringify(metadata.metadata, null, 2) + "</p>";
        }

        // Scroll to bottom of chat box
        chatBox.scrollTop = chatBox.scrollHeight;

        // Clear input field
        document.getElementById('chat-input').value = '';
    }
</script>

</body>
</html>
----------------
from flask import Flask, jsonify, request
from pymongo import MongoClient
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
import numpy as np
import uuid  # For handling UUIDs

app = Flask(__name__)

# MongoDB setup (replace with your credentials)
client = MongoClient('mongodb://localhost:27017/')
db = client['dataportal']
collections = db['documents']  # Collection for storing documents
vectors = db['vector']  # Collection for storing document vectors

# Initialize TF-IDF Vectorizer
vectorizer = TfidfVectorizer()

# Function to handle cosine similarity
def calculate_similarity(query_vector, vectors_data):
    return cosine_similarity(query_vector, vectors_data)[0]

# 1. Check if document exists based on UUID in format CDL::UUID
@app.route('/chat', methods=['POST'])
def chat():
    user_input = request.json.get('query')

    # Check if query is in the format 'CDL::UUID'
    if user_input.startswith('CDL::'):
        try:
            # Extract UUID
            uuid_str = user_input.split('::')[1].strip()
            doc_uuid = uuid.UUID(uuid_str)

            # Check if the document exists in MongoDB
            document = collections.find_one({'_id': doc_uuid})

            if document:
                # Document found, ask if user wants metadata
                return jsonify({"response": "Yes, document found. Do you want to see the metadata?"}), 200
            else:
                # Document not found
                return jsonify({"response": "No, document not found."}), 200
        except Exception as e:
            return jsonify({"error": "Invalid UUID format."}), 400
    else:
        return jsonify({"error": "Query not in expected format 'CDL::UUID'."}), 400

# 2. Provide document metadata if requested
@app.route('/metadata', methods=['POST'])
def metadata():
    uid = request.json.get('uid')

    try:
        doc_uuid = uuid.UUID(uid)

        # Fetch full document from MongoDB
        document = collections.find_one({'_id': doc_uuid})

        if document:
            return jsonify({"metadata": document}), 200
        else:
            return jsonify({"error": "Document not found."}), 404
    except Exception as e:
        return jsonify({"error": "Invalid UUID format."}), 400

if __name__ == '__main__':
    app.run(debug=True)
