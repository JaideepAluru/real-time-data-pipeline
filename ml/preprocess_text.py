import pandas as pd
import nltk
from sklearn.feature_extraction.text import TfidfVectorizer
from nltk.tokenize import word_tokenize

# Download necessary NLTK resources
nltk.download('punkt')
nltk.download('stopwords')

# Sample raw text data (you can replace this with your actual data source)
data = pd.DataFrame({'text': ['I love this product!', 'This is a bad experience.', 'Great service.']})

# Step 1: Tokenize the text
def tokenize_text(text):
    tokens = word_tokenize(text.lower())  # Tokenize and convert to lowercase
    return " ".join(tokens)

data['tokens'] = data['text'].apply(tokenize_text)

# Step 2: Vectorize using TF-IDF
vectorizer = TfidfVectorizer(stop_words='english')  # Removing stopwords from vectorization
X = vectorizer.fit_transform(data['tokens'])

# Step 3: Convert the result to a DataFrame
tfidf_df = pd.DataFrame(X.toarray(), columns=vectorizer.get_feature_names_out())

# Step 4: Save the processed data (for example, as CSV)
tfidf_df.to_csv('preprocessed_data.csv', index=False)

print("Text preprocessing complete! Data saved to 'preprocessed_data.csv'.")
