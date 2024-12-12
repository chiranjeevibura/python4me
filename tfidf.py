import os
import pymongo
import fitz  # PyMuPDF
from sklearn.feature_extraction.text import TfidfVectorizer
import numpy as np

# MongoDB Setup
client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client["resume_db"]
collection = db["resumes"]

# TF-IDF Vectorizer
vectorizer = TfidfVectorizer(stop_words='english')

# Function to extract text from PDFs
def extract_text_from_pdf(pdf_path):
    doc = fitz.open(pdf_path)
    text = ""
    for page in doc:
        text += page.get_text()
    return text

# Function to chunk text into smaller parts
def chunk_text(text, chunk_size=500):
    words = text.split()
    chunks = []
    for i in range(0, len(words), chunk_size):
        chunks.append(" ".join(words[i:i+chunk_size]))
    return chunks

# Function to generate and store vector embeddings in MongoDB
def store_resume_in_db(pdf_path):
    text = extract_text_from_pdf(pdf_path)
    chunks = chunk_text(text)

    # Generate TF-IDF vectors for each chunk
    tfidf_matrix = vectorizer.fit_transform(chunks)

    # Store the resume with embeddings in MongoDB
    resume_data = {
        "file_name": os.path.basename(pdf_path),
        "embeddings": tfidf_matrix.toarray().tolist()  # Convert to list for MongoDB storage
    }
    
    collection.insert_one(resume_data)

# Function to query resumes by skill (using embeddings)
def query_resumes_by_skill(skill):
    # Convert the query skill into a TF-IDF vector
    query_vector = vectorizer.transform([skill]).toarray()

    results = []

    for resume in collection.find():
        embeddings = np.array(resume["embeddings"])

        # Compute cosine similarity between query vector and each chunk's vector
        similarities = np.dot(embeddings, query_vector.T)
        best_match_idx = np.argmax(similarities)

        results.append({
            "file_name": resume["file_name"],
            "best_match_chunk": chunks[best_match_idx],
            "similarity_score": similarities[best_match_idx]
        })

    # Sort results based on similarity score
    results = sorted(results, key=lambda x: x["similarity_score"], reverse=True)
    
    return results[:5]  # Return the top 5 resumes

# Example Usage
pdf_folder = "C:/Users/Chiranjeevi/Documents/resumes"
for pdf_file in os.listdir(pdf_folder):
    if pdf_file.endswith(".pdf"):
        store_resume_in_db(os.path.join(pdf_folder, pdf_file))

# Query resumes for "Java"
skill = "Java"
top_resumes = query_resumes_by_skill(skill)
print(top_resumes)
