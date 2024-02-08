import psycopg2
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.metrics.pairwise import cosine_similarity
import json
import schedule
import time
from DAL import DAL

dal = DAL()

# Function to calculate cosine similarity
def calculate_cosine_similarity(text1, text2):
    vectorizer = CountVectorizer()
    vectors = vectorizer.fit_transform([text1, text2])
    similarity = cosine_similarity(vectors)
    return similarity[0, 1]

def update_similarity():
    dal.connection_open()
    dal.cursor.execute("SELECT id, summarized_content FROM similar_news")
    rows = dal.cursor.fetchall()

    threshold = 0.6
    similar_ids_dict = {}

    for row in rows:
        text_id, text = row
        similar_ids = []

        for other_row in rows:
            other_text_id, other_text = other_row
            if text_id != other_text_id:
                similarity = calculate_cosine_similarity(text, other_text)
                if similarity > threshold:
                    similar_ids.append(other_text_id)

        similar_ids_dict[text_id] = similar_ids

    for row in rows:
        text_id, _ = row
        similar_ids = similar_ids_dict[text_id]
        similar_ids_json = json.dumps(similar_ids)

        dal.cursor.execute("UPDATE similar_news SET similar_data = %s WHERE id = %s", (similar_ids_json, text_id))
        dal.connection.commit()

    dal.connection_close()

schedule.every().hour.do(update_similarity)

while True:
    schedule.run_pending()
    time.sleep(1)
