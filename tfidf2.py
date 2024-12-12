import pymongo
import fitz  # PyMuPDF
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
import os

# MongoDB Setup
client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client["bank_contracts_db"]
collection = db["contracts"]

# Function to extract and chunk text from PDFs
def extract_and_chunk_pdf(pdf_path, chunk_size=500):
    doc = fitz.open(pdf_path)
    text = ""
    
    # Extract text from each page of the PDF
    for page in doc:
        text += page.get_text()
    
    # Simple chunking: split text into chunks of a fixed size
    chunks = [text[i:i+chunk_size] for i in range(0, len(text), chunk_size)]
    return chunks

# Function to store contracts and embeddings in MongoDB
def store_contracts_in_db(pdf_folder):
    vectorizer = TfidfVectorizer(stop_words='english')
    pdf_files = [f for f in os.listdir(pdf_folder) if f.endswith(".pdf")]
    
    for pdf_file in pdf_files:
        # Read and chunk the PDF
        chunks = extract_and_chunk_pdf(os.path.join(pdf_folder, pdf_file))
        
        # Vectorize the chunks and compute TF-IDF embeddings
        tfidf_matrix = vectorizer.fit_transform(chunks)
        
        # Store the contract, its chunks, and embeddings in MongoDB
        embeddings = tfidf_matrix.toarray().tolist()  # Convert to list for MongoDB storage
        
        # Document to store
        contract_data = {
            "file_name": pdf_file,
            "chunks": chunks,
            "embeddings": embeddings
        }
        
        collection.insert_one(contract_data)
    
    print("Contracts stored in MongoDB.")

# Store documents in MongoDB
store_contracts_in_db("dummy_contract_pdfs")


def retrieve_relevant_documents(query, top_k=3):
    # Extract embeddings from MongoDB
    contract_documents = collection.find()
    
    # Convert all documents into a list of text chunks (we will use their embeddings)
    documents = []
    document_embeddings = []
    for doc in contract_documents:
        documents.extend(doc["chunks"])
        document_embeddings.extend(doc["embeddings"])

    # Fit vectorizer on the combined documents (query + stored documents)
    all_texts = documents + [query]
    vectorizer = TfidfVectorizer(stop_words='english')
    tfidf_matrix = vectorizer.fit_transform(all_texts)  # Fit on combined text

    # Query vector is the last row (from the input query)
    query_vector = tfidf_matrix[-1]  # The query is the last row
    document_vectors = tfidf_matrix[:-1]  # The documents are the first n rows

    # Calculate cosine similarity between the query and all document vectors
    similarity_scores = cosine_similarity(query_vector, document_vectors)
    
    # Get top-k most relevant documents
    sorted_indices = similarity_scores[0].argsort()[::-1]
    top_indices = sorted_indices[:top_k]
    
    print("Top Relevant Documents:")
    for idx in top_indices:
        print(f"Document: {documents[idx]}")
        print(f"Similarity Score: {similarity_scores[0][idx]:.4f}")
        print("-" * 50)

# Example Query
query = "loan amount and interest rate"
retrieve_relevant_documents(query)
