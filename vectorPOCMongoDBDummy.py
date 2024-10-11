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
