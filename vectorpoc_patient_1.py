from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity

# Example patient diagnostic records
documents = [
    "John, 45 years old, diagnosed with diabetes, treated by Dr. Smith at City Hospital. Treatment includes insulin therapy.",
    "Mary, 60 years old, diagnosed with hypertension, treated by Dr. Johnson at Green Valley Hospital. Treatment includes blood pressure medications.",
    "Alice, 35 years old, diagnosed with asthma, treated by Dr. Lee at Metro Clinic. Treatment includes inhalers and regular check-ups.",
    "Bob, 50 years old, diagnosed with heart disease, treated by Dr. Kim at River Health Center. Treatment includes lifestyle changes and medications.",
    "Charlie, 25 years old, diagnosed with anxiety disorder, treated by Dr. Williams at Central Health Clinic. Treatment includes therapy and medications.",
    "David, 40 years old, diagnosed with chronic back pain, treated by Dr. Patel at Lakeside Hospital. Treatment includes physical therapy.",
    "Emma, 30 years old, diagnosed with pneumonia, treated by Dr. Brown at Mountainview Hospital. Treatment includes antibiotics.",
    "Frank, 55 years old, diagnosed with arthritis, treated by Dr. Clark at Sunshine Clinic. Treatment includes pain relief medications.",
    "Grace, 70 years old, diagnosed with Alzheimer's, treated by Dr. Davis at Coastal Health Center. Treatment includes cognitive therapy.",
    "Hannah, 48 years old, diagnosed with depression, treated by Dr. Evans at Pinewood Hospital. Treatment includes counseling and medications.",
    "Ivy, 65 years old, diagnosed with osteoporosis, treated by Dr. Martinez at Horizon Clinic. Treatment includes calcium supplements and exercise.",
    "Jack, 38 years old, diagnosed with migraines, treated by Dr. Young at Crossroads Medical Center. Treatment includes migraine management medications.",
    "Karen, 42 years old, diagnosed with thyroid disorder, treated by Dr. Parker at Redwood Hospital. Treatment includes thyroid hormone replacement therapy.",
    "Luke, 29 years old, diagnosed with skin allergies, treated by Dr. Rivera at Oakridge Clinic. Treatment includes antihistamines.",
    "Mia, 50 years old, diagnosed with chronic kidney disease, treated by Dr. Rodriguez at Sunrise Medical Center. Treatment includes dialysis.",
    "Nathan, 55 years old, diagnosed with liver disease, treated by Dr. Morgan at Harborview Hospital. Treatment includes lifestyle changes and medications.",
    "Olivia, 33 years old, diagnosed with endometriosis, treated by Dr. Carter at Blossom Women's Health. Treatment includes hormonal therapy.",
    "Paul, 47 years old, diagnosed with glaucoma, treated by Dr. Bailey at Evergreen Eye Care. Treatment includes eye drops and surgery.",
    "Quinn, 52 years old, diagnosed with prostate cancer, treated by Dr. Perez at Kingsley Hospital. Treatment includes chemotherapy and radiation therapy.",
    "Rachel, 68 years old, diagnosed with cataracts, treated by Dr. Adams at Willow Eye Clinic. Treatment includes cataract surgery."
]

# Create the TfidfVectorizer
vectorizer = TfidfVectorizer(stop_words='english')

# Fit and transform the patient diagnostic documents into vectors
tfidf_matrix = vectorizer.fit_transform(documents)

# Example query to find similar patient records
query = ["Find patients treated by Dr. Smith for diabetes."]
query_vector = vectorizer.transform(query)

# Compute cosine similarity between the query and patient diagnostics
query_sim = cosine_similarity(query_vector, tfidf_matrix)

# Find the most similar document
most_similar_idx = query_sim.argmax()
print(f"Most similar document index: {most_similar_idx}")
print(f"Most similar document: {documents[most_similar_idx]}")
