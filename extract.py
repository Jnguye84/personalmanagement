import os
from neo4j import GraphDatabase
from pdf2image import convert_from_path
import pytesseract
import json
from PIL import Image
import sys
import concurrent.futures

pytesseract.pytesseract.tesseract_cmd = r'C:\Program Files\Tesseract-OCR\tesseract.exe'
os.environ['TESSDATA_PREFIX'] = r'C:\Program Files\Tesseract-OCR\tessdata'

uri = "bolt://localhost:7687"
AUTH = ("neo4j", "password")
driver = GraphDatabase.driver(uri, auth=AUTH)

create_query = """
UNWIND $data AS row
WITH row
WHERE row.FileName IS NOT NULL AND trim(row.FileName) <> ''
MERGE (d:Title {name: row.FileName})
WITH d, row
UNWIND split(trim(row.Keywords), ' ') AS keyword
MERGE (k:Keyword {name: keyword})
SET k.sentences = row.Sentences  // Set the 'sentences' property on the Keyword node
MERGE (d)-[:HAS_KEYWORD]->(k)
RETURN *
"""

check_titles_query = """
MATCH (t:Title)
RETURN t.name AS FileName
"""

query_match = """
MATCH (t:Title)-[:HAS_KEYWORD]->(k:Keyword)
WHERE k.sentences CONTAINS $word
RETURN t.name AS FileName, k.sentences AS Sentences
"""

def process_word_in_sentence(word, word_list_sentences):
    result = {}
    for sentence in word_list_sentences:
        if word in sentence:
            result[word] = sentence
            break  # Once a match is found, no need to check other sentences
    return result

#each new row is a new document
def putting_text_in_graph(text, file_path):

    word_list_sentences = text.split('.')
    word_list = set(word.lower() for word in text.split() if word.isalpha())  # Using set to avoid duplicate words
    
    # Create a dictionary mapping words to sentences
    dict_word_sentence = {}
    with concurrent.futures.ThreadPoolExecutor() as executor:
            futures = {executor.submit(process_word_in_sentence, word, word_list_sentences): word for word in word_list}
            
            # Wait for all the futures to complete and gather the results
            for future in concurrent.futures.as_completed(futures):
                word = futures[future]
                try:
                    result = future.result()
                    dict_word_sentence.update(result)  # Merge the results into dict_word_sentence
                except Exception as exc:
                    print(f"Error processing word {word}: {exc}")
    # Prepare new rows (for batch insertion later)
    rows = []
    for word, sentence in dict_word_sentence.items():
        new_row = {'FileName': file_path, 'Keywords': word, 'Sentences': sentence}
        print(new_row)
        rows.append(new_row)
    
    # Batch database operations to minimize session usage
    with driver.session() as session:
        for new_row in rows:
            # Execute queries in one session
            session.run(create_query, data=new_row, rawtext=text)

def process_file(file_path, file_extenion):
    text = ""
    if file_extension.lower() in ['.png', '.jpg', '.jpeg']:
        img = Image.open(file_path)
        text = pytesseract.image_to_string(img, lang='eng').strip()
    elif file_extension.lower() == '.txt':
        with open(file_path, "r", encoding="utf-8") as file:
            text = file.read()
    elif file_extension.lower() == '.pdf':
        text = ''
        doc = convert_from_path(file_path)  
        for page in doc:
            text = pytesseract.image_to_string(page)
            text += text + '\n'
    return text

def process_and_insert(file_path, file_extension):
    text = process_file(file_path, file_extension)
    if text:
        putting_text_in_graph(text, file_path)
    else:
        print(f"No text detected in {file_path}.")

# Start of Code / End of Functions

DOCUMENT_PATH_FILE = r"C:\Users\Owner\documentpath.txt"
with open(DOCUMENT_PATH_FILE, "r") as f:
    document_path = f.read().strip()

#this is to check all the existing files within the database
with driver.session() as session:
    content = session.run(check_titles_query).data()
    file_contents = [record['FileName'] for record in content]

with concurrent.futures.ThreadPoolExecutor() as executor:
    futures = []
    #this is to add any new files that were added within the folder
    for root, _, files in os.walk(document_path):
        for file in files:
            file_path = os.path.join(root, file)
            file_name, file_extension = os.path.splitext(file)
            if file_path not in file_contents:
                futures.append(executor.submit(process_and_insert, file_path, file_extension))
    
    concurrent.futures.wait(futures)

query = None
if len(sys.argv) > 1:
  query = sys.argv[1]

def chain(query):
    with driver.session() as session:
        result = session.run(query_match, word = query)
        result_dict = [{'FileName': record['FileName'], 'Sentences': record['Sentences']} for record in result]
    return result_dict

#Beginning of User Prompting

# Ask the user if they want to enter a new document path or keep the current one
doc_choice = input(f"Current document path: {document_path}\nPress 'n' for a new document path or 'o' to keep the current one: ").strip().lower()

if doc_choice == 'n':
    document_path = input("Enter the new document path: ").strip()
    
    # Save the new document path to the file
    with open(DOCUMENT_PATH_FILE, "w") as f:
        f.write(document_path)
    
    print(f"Document path updated to: {document_path}")

elif doc_choice == 'o':
    print(f"Keeping the current document path: {document_path}")

elif doc_choice in ['quit', 'q', 'exit']:
    sys.exit()
else:
    print("Invalid choice. Please enter 'n' or 'o'.")
    sys.exit()

while True:
    if not query:
        query = input("Find me documents that have the word: ")

    if query in ['quit', 'q', 'exit']:
        sys.exit()

    result = chain(query)
    for r in result:
        if isinstance(r, dict):  
            print(f"{r['FileName']}\t{r['Sentences']}\n")
        else:
            print("Unexpected data format:", r) 

    query = None  # Reset query for the next loop
