import pymongo
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
import fitz  # PyMuPDF for reading PDF files
import os
import numpy as np
import scipy.sparse as sp

# MongoDB Setup
client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client["contract_db"]
collection = db["contracts"]

# Function to read PDFs from a folder and extract text
def extract_text_from_pdfs(pdf_folder):
    pdf_files = [f for f in os.listdir(pdf_folder) if f.endswith('.pdf')]
    contract_texts = []
    for pdf_file in pdf_files:
        doc = fitz.open(os.path.join(pdf_folder, pdf_file))
        text = ""
        for page in doc:
            text += page.get_text()
        contract_texts.append((pdf_file, text))  # Store filename and text
    return contract_texts

# Extract text from PDFs
pdf_folder = "path_to_pdf_folder"  # Path to the folder containing PDFs
contract_texts = extract_text_from_pdfs(pdf_folder)

# Step 1: Define chunking strategy (section-wise)
def chunk_contract(contract_text):
    # Simple chunking by sections (can be enhanced with more complex rules)
    sections = contract_text.split("\n\n")  # Assuming paragraphs are separated by two newlines
    return sections

# Step 2: Initialize the TF-IDF vectorizer
vectorizer = TfidfVectorizer(stop_words='english')

# Step 3: Loop through each contract, chunk it, and create TF-IDF embeddings
for pdf_file, contract_text in contract_texts:
    contract_chunks = chunk_contract(contract_text)
    
    # Fit and transform the chunks into TF-IDF vectors
    tfidf_matrix = vectorizer.fit_transform(contract_chunks)
    
    # Convert the TF-IDF matrix to a sparse array (sparse format for better memory management)
    embedding_sparse = sp.csr_matrix(tfidf_matrix)
    
    # Store the contract data in MongoDB
    contract_data = {
        "contract_title": pdf_file,
        "contract_text": contract_text,
        "embeddings": embedding_sparse.tolist()  # Storing sparse matrix as a list
    }
    
    collection.insert_one(contract_data)

# Step 4: Perform query search to retrieve the most similar contract
query_text = "loan amount and interest rate"
query_vector = vectorizer.transform([query_text])

# Step 5: Calculate cosine similarity between the query and stored contract embeddings
for contract in collection.find():
    contract_embedding = np.array(contract["embeddings"]).reshape(1, -1)
    
    # Cosine similarity calculation
    similarity_score = cosine_similarity(query_vector, contract_embedding)[0][0]
    
    print(f"Contract Title: {contract['contract_title']}, Similarity Score: {similarity_score:.4f}")

# Step 6: Indexing to speed up retrieval and searches
collection.create_index([("contract_title", pymongo.ASCENDING)])
collection.create_index([("embeddings", pymongo.ASCENDING)])

print("Contract documents and embeddings have been stored and indexed.")
