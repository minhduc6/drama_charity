import uuid

from pymongo import MongoClient
import unidecode
import csv


def save_to_mongodb(combined_data, database_name, collection_name):
    mongo_uri_with_auth = f"mongodb://root:123456@localhost:27017/"
    # Connect to MongoDB

    try:
        client = MongoClient(mongo_uri_with_auth)
        # Check if the connection was successful
        if client is not None:
            print("Connected to MongoDB successfully.")
    except Exception as e:
        print(f"Failed to connect to MongoDB: {e}")
        return

    # Access the specified database
    db = client[database_name]
    # Access the specified collection
    collection = db[collection_name]

    # Insert combined data into the collection
    for item in combined_data:
        collection.insert_one(item)

    # Close the connection
    client.close()


def normalize_vietnamese(text):
    # Remove accents
    text = unidecode.unidecode(text)
    # Convert to lowercase
    text = text.lower()
    # Additional normalization steps if needed

    return text


def read_csv_file(file_path):
    data = []
    with open(file_path, newline='', encoding='utf-8') as csvfile:
        # Create a CSV reader object
        csv_reader = csv.reader(csvfile)

        # Iterate over each row in the CSV file
        for row in csv_reader:
            # Print each row
            data.append(
                {
                    "id": str(uuid.uuid4()),
                    "data": row
                }
            )
    return data


def tranform_data(data):

    # lower case
    tranform_data = []
    for item in data:
        try:
            if isinstance(item.get("data")[0], str):
                tranform_data.append(
                    {
                        "id": item.get("id"),
                        "data": normalize_vietnamese(item.get("data")[0])
                    }
                )
        except IndexError:
            # Skip items where index 0 is out of range
            pass

    # filter thuy tien and cong vinh and tu thien
    filter_data = []
    for item in tranform_data:
        if 'thuy tien' in item.get("data") and 'cong vinh' in item.get("data") and 'tu thien' in item.get("data"):
            filter_data.append(item)

    # Combine data and filter_data based on the same id
    combined_data = []
    for item in data:
        for filtered_item in filter_data:
            if item.get("id") == filtered_item.get("id"):
                combined_data.append({
                    "id": item.get("id"),
                    "data": item.get("data")[0],
                    "unicode_data": filtered_item.get("data")
                })

    return combined_data


if __name__ == '__main__':
    # connect_to_mongodb()
    data = read_csv_file("200k_comments.csv")
    result_data = tranform_data(data)
    save_to_mongodb(combined_data=result_data, database_name="comment", collection_name="drama_tuthien")
