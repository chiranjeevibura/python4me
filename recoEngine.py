from pymongo import MongoClient
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score

# MongoDB connection
client = MongoClient("mongodb://localhost:27017/")
db = client['document_db']
collection = db['document_metadata']

# Function to fetch metadata from MongoDB
def fetch_metadata_from_mongo():
    cursor = collection.find({}, {'DOMAIN': 1, 'DIVISION': 1, 'SUBDIVISION': 1, 'SECURITY_PROXY_NAME': 1, 'RECORD_CODE': 1, '_id': 0})
    metadata = list(cursor)
    return pd.DataFrame(metadata)

# Data preprocessing (encoding categorical variables)
def preprocess_data(df):
    df = df.dropna(subset=['RECORD_CODE'])  # Remove rows without RECORD_CODE for training
    df_encoded = pd.get_dummies(df[['DOMAIN', 'DIVISION', 'SUBDIVISION', 'SECURITY_PROXY_NAME']])
    X = df_encoded
    y = df['RECORD_CODE']
    return X, y

# Function to train a model to predict RECORD_CODE
def train_model(X, y):
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
    model = RandomForestClassifier(random_state=42)
    model.fit(X_train, y_train)
    
    # Evaluate the model
    y_pred = model.predict(X_test)
    print(f"Model Accuracy: {accuracy_score(y_test, y_pred) * 100:.2f}%")
    return model

# Function to predict RECORD_CODE for new metadata
def predict_record_code(model, new_metadata):
    new_metadata_encoded = pd.get_dummies(new_metadata)
    # Align new data with training data columns
    new_metadata_encoded = new_metadata_encoded.reindex(columns=model.feature_names_in_, fill_value=0)
    predicted_code = model.predict(new_metadata_encoded)
    return predicted_code

# Fetching data from MongoDB
metadata_df = fetch_metadata_from_mongo()

# Preprocessing the data for training
X, y = preprocess_data(metadata_df)

# Training the model
model = train_model(X, y)

# Example of new document metadata without RECORD CODE
new_document_metadata = pd.DataFrame([{
    "DOMAIN": "Finance",
    "DIVISION": "Accounting",
    "SUBDIVISION": "Payroll",
    "SECURITY_PROXY_NAME": "Level-3"
}])

# Predicting RECORD CODE for the new document
predicted_code = predict_record_code(model, new_document_metadata)
print(f"Predicted RECORD CODE: {predicted_code[0]}")


# Function to update MongoDB with predicted RECORD_CODE
def update_mongo_with_predicted_code(doc_id, predicted_code):
    collection.update_one(
        {"_id": doc_id},
        {"$set": {"RECORD_CODE": predicted_code}}
    )

# Example of updating MongoDB for a document without RECORD_CODE
doc_id = some_document_id  # Replace with actual document ID
predicted_code = predict_record_code(model, new_document_metadata)
update_mongo_with_predicted_code(doc_id, predicted_code[0])


######## Notes:
Yes, it's definitely possible to recommend or even autopopulate the RECORD CODE based on existing metadata stored in MongoDB, such as DOMAIN, DIVISION, SUBDIVISION, and SECURITY_PROXY_NAME. The idea here is to build a system that can predict or suggest a RECORD CODE for a document based on patterns learned from similar documents' metadata.

High-Level Approach:
We can approach this problem using machine learning or rule-based methods, depending on the complexity of the data and the relationships between the metadata and RECORD CODE. Here's how you can proceed:

1. Data Preprocessing:
Extract metadata and associated RECORD CODE from MongoDB.
Clean and structure the data for processing. This could include categorical encoding for DOMAIN, DIVISION, etc.
2. Model Building for Prediction:
We can use a supervised machine learning model to predict the RECORD CODE based on the metadata. Some possible models:

Decision Trees or Random Forests: These are good for categorical data like the metadata you have.
Naive Bayes: A simple yet effective model for categorical data.
Logistic Regression: For simple linear relationships between metadata features and RECORD CODE.
Neural Networks: If the dataset is complex and large enough.
3. Autopopulation:
Once the model is trained, it can be used to autopopulate missing RECORD CODEs for incoming documents. If the RECORD CODE is missing in the metadata, the model will predict and suggest it.

4. Recommendation System:
In addition to autopopulation, you can also recommend the top N likely RECORD CODEs based on the metadata using probabilistic methods or by ranking model predictions.

Let’s walk through the steps and code for implementing a system like this:

Full Working Code
>>>>shared above>>>>
Breakdown of the Code:
Fetching Metadata from MongoDB: The fetch_metadata_from_mongo function retrieves the required fields (DOMAIN, DIVISION, SUBDIVISION, SECURITY_PROXY_NAME, and RECORD_CODE) from MongoDB.

Preprocessing: The preprocess_data function handles categorical encoding using pd.get_dummies, which converts the categorical metadata into binary form suitable for machine learning models. The RECORD_CODE serves as the target variable (y).

Model Training: In train_model, a RandomForestClassifier is trained to predict RECORD_CODE based on the encoded metadata. The training and testing sets are split, and the model’s accuracy is displayed.

Prediction: The predict_record_code function takes in new metadata (without a RECORD CODE) and uses the trained model to predict the most likely RECORD CODE.

Scenarios and Extensions:
Bulk Autopopulation:

This system can be extended to autopopulate RECORD CODEs for a batch of documents where the RECORD CODE is missing. For each document with missing code, the system can predict one and update the database.
Top-N Recommendations:

Instead of predicting just one RECORD CODE, you can modify the model to return the top-N likely RECORD CODEs by extracting probabilities from the classifier (e.g., using model.predict_proba() in RandomForestClassifier).
Handling New Metadata:

For unseen metadata values (e.g., new DOMAIN or DIVISION), the system will handle them as unknown categories. You can add specific logic to handle such cases (e.g., using placeholder categories or default values).
Continuous Learning:

The model can be continuously trained as new documents and their RECORD CODEs are added to the system, improving the accuracy and making better recommendations over time.
MongoDB Integration:
To integrate the recommendation system with MongoDB for updating or autopopulating RECORD CODEs, you can write the prediction results back to MongoDB:
>>>>shared above>>>>
This function update_mongo_with_predicted_code would update the document in MongoDB, setting the predicted RECORD CODE in the document’s metadata.

Conclusion:
By using machine learning to predict and recommend RECORD CODEs, you can automate metadata management and recommendation for new documents in your system. This is especially useful for large-scale document repositories like your Hitachi storage system, where metadata consistency is crucial.

Would you like to explore more on any specific aspect or extend the system further?
