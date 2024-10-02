import nltk
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
from nltk.stem import WordNetLemmatizer
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
from pymongo import MongoClient
import PyPDF2
import numpy as np

# Download NLTK resources
nltk.download('punkt')
nltk.download('stopwords')
nltk.download('wordnet')

# Preprocess text (tokenization, stopwords removal, and lemmatization)
def preprocess_text(text):
    stop_words = set(stopwords.words('english'))
    word_tokens = word_tokenize(text.lower())
    
    lemmatizer = WordNetLemmatizer()
    cleaned_tokens = [lemmatizer.lemmatize(w) for w in word_tokens if w.isalpha() and w not in stop_words]
    
    return ' '.join(cleaned_tokens)

# Read PDF and extract text
def extract_text_from_pdf(pdf_path):
    text = ""
    try:
        with open(pdf_path, 'rb') as file:
            reader = PyPDF2.PdfReader(file)
            for page_num in range(len(reader.pages)):
                text += reader.pages[page_num].extract_text()
    except Exception as e:
        print(f"Error reading PDF: {e}")
    return text

# Vectorize the text chunks using TF-IDF
def vectorize_documents(documents):
    vectorizer = TfidfVectorizer()
    vectors = vectorizer.fit_transform(documents)  # Generate TF-IDF vectors
    return vectors, vectorizer

# Insert document chunk and vector into MongoDB
def insert_document_to_mongodb(document_id, chunk, vector, collection):
    doc = {
        "document_id": document_id,
        "text_chunk": chunk,
        "vector": vector.tolist(),  # Store vector as list
    }
    collection.insert_one(doc)

# Process and store PDF document
def process_and_store_pdf(document_id, pdf_path, collection):
    # Extract text from the PDF file
    document_text = extract_text_from_pdf(pdf_path)
    
    if not document_text:
        print("Error: No text found in the PDF.")
        return
    
    # Split the document into paragraphs (you can modify to split by sentences)
    chunks = document_text.split('\n')  # Split by new line (modify as needed)
    preprocessed_chunks = [preprocess_text(chunk) for chunk in chunks if chunk.strip()]  # Ensure non-empty chunks
    
    # Vectorize all preprocessed chunks
    vectors, vectorizer = vectorize_documents(preprocessed_chunks)
    
    # Store each chunk along with its vector into MongoDB
    for i, (chunk, vector) in enumerate(zip(chunks, vectors.toarray())):
        insert_document_to_mongodb(f"{document_id}_{i}", chunk, vector, collection)
    
    print(f"Document '{document_id}' has been successfully processed and stored in MongoDB.")

# Search similar chunks by query
def search_similar_documents(query, vectorizer, collection):
    query_vector = vectorizer.transform([preprocess_text(query)]).toarray()[0]
    
    # Retrieve all stored vectors and chunks from MongoDB
    docs = list(collection.find({}, {"text_chunk": 1, "vector": 1}))
    vectors = np.array([doc['vector'] for doc in docs])
    
    # Calculate cosine similarity between query vector and stored vectors
    similarities = cosine_similarity([query_vector], vectors)[0]
    
    # Sort documents by similarity
    results = sorted(zip(docs, similarities), key=lambda x: x[1], reverse=True)
    
    # Display top 5 results
    print("Top 5 most similar chunks:")
    for i, (doc, similarity) in enumerate(results[:5]):
        print(f"Rank {i+1}: Similarity={similarity:.2f}")
        print(f"Text Chunk: {doc['text_chunk']}\n")
    
# Example usage
if __name__ == "__main__":
    # MongoDB connection
    client = MongoClient('mongodb://localhost:27017/')
    db = client['vector_db']  # Database
    collection = db['documents']  # Collection
    
    # Process and store PDF document
    document_id = "sample_pdf_doc"
    pdf_path = "/path/to/your/document.pdf"  # Replace with your PDF path
    process_and_store_pdf(document_id, pdf_path, collection)
    
    # Search for similar documents using a query
    search_query = "Your search query here"  # Replace with your search query
    # You can use the vectorizer from the previous processing step
    search_similar_documents(search_query, TfidfVectorizer(), collection)


---------------

from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
from pymongo import MongoClient
import PyPDF2
import numpy as np
import re

# Basic text preprocessing: remove non-alphabetic characters, lowercase, and split into words
def preprocess_text_basic(text):
    # Remove non-alphabetic characters using regex
    text = re.sub(r'[^a-zA-Z\s]', '', text)
    # Convert to lowercase and split into words (basic tokenization)
    return text.lower().split()

# Read PDF and extract text
def extract_text_from_pdf(pdf_path):
    text = ""
    try:
        with open(pdf_path, 'rb') as file:
            reader = PyPDF2.PdfReader(file)
            for page_num in range(len(reader.pages)):
                text += reader.pages[page_num].extract_text()
    except Exception as e:
        print(f"Error reading PDF: {e}")
    return text

# Vectorize the text chunks using TF-IDF
def vectorize_documents(documents):
    # Use built-in tokenization and stopword removal in TfidfVectorizer
    vectorizer = TfidfVectorizer(tokenizer=preprocess_text_basic, stop_words='english')
    vectors = vectorizer.fit_transform(documents)  # Generate TF-IDF vectors
    return vectors, vectorizer

# Insert document chunk and vector into MongoDB
def insert_document_to_mongodb(document_id, chunk, vector, collection):
    doc = {
        "document_id": document_id,
        "text_chunk": chunk,
        "vector": vector.tolist(),  # Store vector as a list to make it compatible with MongoDB
    }
    collection.insert_one(doc)

# Process and store PDF document
def process_and_store_pdf(document_id, pdf_path, collection):
    # Extract text from the PDF file
    document_text = extract_text_from_pdf(pdf_path)
    
    if not document_text:
        print("Error: No text found in the PDF.")
        return
    
    # Split the document into chunks (paragraphs in this case, can modify to split by sentence)
    chunks = document_text.split('\n')  # Split by new line or modify for more granular splitting
    preprocessed_chunks = [chunk.strip() for chunk in chunks if chunk.strip()]  # Remove empty chunks
    
    # Vectorize all preprocessed chunks
    vectors, vectorizer = vectorize_documents(preprocessed_chunks)
    
    # Store each chunk along with its vector into MongoDB
    for i, (chunk, vector) in enumerate(zip(preprocessed_chunks, vectors.toarray())):
        insert_document_to_mongodb(f"{document_id}_{i}", chunk, vector, collection)
    
    print(f"Document '{document_id}' has been successfully processed and stored in MongoDB.")

# Search similar chunks by query
def search_similar_documents(query, vectorizer, collection):
    # Vectorize the search query using the same vectorizer as the documents
    query_vector = vectorizer.transform([query]).toarray()[0]
    
    # Retrieve all stored vectors and chunks from MongoDB
    docs = list(collection.find({}, {"text_chunk": 1, "vector": 1}))
    vectors = np.array([doc['vector'] for doc in docs])
    
    # Calculate cosine similarity between query vector and stored vectors
    similarities = cosine_similarity([query_vector], vectors)[0]
    
    # Sort documents by similarity
    results = sorted(zip(docs, similarities), key=lambda x: x[1], reverse=True)
    
    # Display top 5 results
    print("Top 5 most similar chunks:")
    for i, (doc, similarity) in enumerate(results[:5]):
        print(f"Rank {i+1}: Similarity={similarity:.2f}")
        print(f"Text Chunk: {doc['text_chunk']}\n")

# Example usage
if __name__ == "__main__":
    # MongoDB connection
    client = MongoClient('mongodb://localhost:27017/')
    db = client['vector_db']  # Database
    collection = db['documents']  # Collection
    
    # Process and store PDF document
    document_id = "sample_pdf_doc"
    pdf_path = "/path/to/your/document.pdf"  # Replace with your PDF path
    process_and_store_pdf(document_id, pdf_path, collection)
    
    # Search for similar documents using a query
    search_query = "Your search query here"  # Replace with your search query
    search_similar_documents(search_query, TfidfVectorizer(tokenizer=preprocess_text_basic, stop_words='english'), collection)

