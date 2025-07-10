import fitz  # PyMuPDF
import os
import re 
import pyodbc
import configparser

pdf_path = "Dhivakar_Resume.pdf"


if not os.path.exists(pdf_path):
    print("File not found!")
else:
    # Step 1: Extract text
    doc = fitz.open(pdf_path)
    text = ""
    for page in doc:
        text += page.get_text()

    # Step 2: Split into lines for analysis
    lines = text.strip().split('\n')
    print(lines)

#1.Name Extraction
    # Step 3: Heuristic to detect name
    probable_name = None
    for line in lines[:10]:  # Check only top 10 lines
        words = line.strip().split()
        if len(words) >= 1 and all(w[0].isupper() and w.isalpha() for w in words):
            probable_name = line.strip()
            break

    if probable_name:
        print("Name: ", probable_name)
    else:
        print(" Name not found using basic rules.")
    
#2.Phone Extraction
    phone_match = re.search(r'(?<!\+)(?<!\d)(\d{10})(?!\d)', text)
    phone = phone_match.group(1).strip() if phone_match else None
    print("Phone:", phone if phone else "Not found")

#3.Email Extraction
    email_match = re.search(r'[\w\.-]+@[\w\.-]+\.\w+', text)
    email = email_match.group(0).strip() if email_match else None
    print("Email:", email if email else "Not found")

#4.LinkedIn ID Extraction
    linkedin_match = re.search(
    r'(https?://(?:www\.)?linkedin\.com/in/[^\s]+|linkedin\.com/in/[^\s]+)',
    text, re.IGNORECASE
    )
    linkedin = linkedin_match.group(1).strip() if linkedin_match else None
    print("LinkedIn:", linkedin if linkedin else "Not found")
    
#5.Skills Extraction
    lines = text.strip().split('\n')

    first_programming_lang = None
    first_spoken_lang = None
    found_skills = False


        # Check for Programming Languages
    known_programming_langs = ["Python", "Java", "C", "C++", "C#", "JavaScript", "TypeScript", "Go", "Ruby", "Kotlin", "Swift", "PHP", "SQL", "R", "Perl"]
    found_langs = []
    for lang in known_programming_langs:
        if re.search(rf'\b{re.escape(lang)}\b', text, re.IGNORECASE):
            found_langs.append(lang)
    first_programming_lang = found_langs[0] if found_langs else "None"
        
    known_tools = ["colab","git", "AWS", "Visual Studio Code", "UI"] 
    found_tools = []
    for tool in known_tools:
        if re.search(rf'\b{re.escape(tool)}\b', text, re.IGNORECASE):
            found_tools.append(tool)
    first_tool = found_tools[0] if found_tools else "None"
        


    
    print("Programming Language:", first_programming_lang if first_programming_lang else "Not found")
    print("Tools:", first_tool if first_tool else "Not found")


#Loading the extracted data into SQL Server....
config = configparser.ConfigParser()
config.read("../config/config.ini")

server = config['sqlserver']['server']
username = config['sqlserver']['username']
password = config['sqlserver']['password']
database = config['sqlserver']['database']
driver = config['sqlserver']['driver']

conn_str = f"DRIVER={driver};SERVER={server};DATABASE={database};UID={username};PWD={password}"
conn = pyodbc.connect(conn_str)
cursor = conn.cursor() 

cursor.execute("INSERT INTO ResumeInfo (Name, Phone, Email, LinkedIn, Programming_Language, Tool) VALUES (?, ?, ?, ?, ?, ?)", 
               (probable_name, phone, email, linkedin, first_programming_lang, first_tool))
conn.commit()

print("Data successfully retrieved from pdf and loaded into SQL Server....")