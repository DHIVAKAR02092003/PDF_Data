import boto3
import fitz  # PyMuPDF
import io
import re
import pyodbc
import configparser
import os

config = configparser.ConfigParser()
config.read("../config/config.ini")

# Step 1: Load PDF from S3
aws_access_key_id = config['aws']['aws_access_key_id']
aws_secret_access_key = config['aws']['aws_secret_access_key']
bucket_name = config['aws']['bucket_name']
pdf_key = config['aws']['pdf_key']

# Initialize S3 Client
s3 = boto3.client('s3',
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key
)

# Fetch PDF from S3
pdf_object = s3.get_object(Bucket=bucket_name, Key=pdf_key)
pdf_stream = io.BytesIO(pdf_object['Body'].read())

# Open PDF in memory
doc = fitz.open(stream=pdf_stream, filetype="pdf")

# Extract text
text = ""
for page in doc:
    text += page.get_text()

# Step 2: Split into lines for analysis
lines = text.strip().split('\n')

# 1. Name Extraction
probable_name = None
for line in lines[:10]:
    words = line.strip().split()
    if len(words) == 2 and all(w[0].isupper() and w.isalpha() for w in words):
        probable_name = line.strip()
        break
print("Name:", probable_name if probable_name else "Not found")

# 2. Phone Extraction
#phone_match = re.search(r'Phone[:\-]?\s*(\+?\d[\d\s\-]{9,15})', text, re.IGNORECASE)
#phone = phone_match.group(1).strip() if phone_match else None
#print("Phone:", phone if phone else "Not found")
phone_match = re.search(r'(?<!\+)(?<!\d)(\d{10})(?!\d)', text)
phone = phone_match.group(1).strip() if phone_match else None
print("Phone:", phone if phone else "Not found")

# 3. Email Extraction
#email_match = re.search(r'Email[:\-]?\s*([\w\.-]+@[\w\.-]+\.\w+)', text, re.IGNORECASE)
#email = email_match.group(1).strip() if email_match else None
#print("Email:", email if email else "Not found")
email_match = re.search(r'[\w\.-]+@[\w\.-]+\.\w+', text)
email = email_match.group(0).strip() if email_match else None
print("Email:", email if email else "Not found")

# 4. LinkedIn Extraction
#linkedin_match = re.search(r'LinkedIn[:\-]?\s*(https?://[^\s]+|linkedin\.com/in/[^\s]+)', text, re.IGNORECASE)
#linkedin = linkedin_match.group(1).strip() if linkedin_match else None
#print("LinkedIn:", linkedin if linkedin else "Not found")
linkedin_match = re.search(
    r'(https?://(?:www\.)?linkedin\.com/in/[^\s]+|linkedin\.com/in/[^\s]+)',
    text, re.IGNORECASE
)
linkedin = linkedin_match.group(1).strip() if linkedin_match else None
print("LinkedIn:", linkedin if linkedin else "Not found")


# 5. Skills Extraction
first_programming_lang = None
first_spoken_lang = None
found_skills = False

for line in lines:
    line = line.strip()

    if line.lower() == "skills":
        found_skills = True
        continue

    if found_skills and line.lower() in ["achievements", "certifications", "projects", "education"]:
        break

    if found_skills and line.startswith("•"):
        line_clean = line.replace("•", "").strip()

        if line_clean.lower().startswith("programming languages"):
            prog_langs = line_clean.split(":", 1)[-1].split(",")
            if prog_langs:
                first_programming_lang = prog_langs[0].strip()

        elif line_clean.lower().startswith("languages"):
            spoken_langs = line_clean.split(":", 1)[-1].split(",")
            if spoken_langs:
                first_spoken_lang = spoken_langs[0].strip()
                first_spoken_lang = re.sub(r"\(.*?\)", "", first_spoken_lang).strip()

    if first_programming_lang and first_spoken_lang:
        break

print("Programming Language:", first_programming_lang if first_programming_lang else "Not found")
print("Spoken Language:", first_spoken_lang if first_spoken_lang else "Not found")

# Step 6: Load to SQL Server

server = config['sqlserver']['server']
username = config['sqlserver']['username']
password = config['sqlserver']['password']
database = config['sqlserver']['database']
driver = config['sqlserver']['driver']

conn_str = f"DRIVER={driver};SERVER={server};DATABASE={database};UID={username};PWD={password}"
conn = pyodbc.connect(conn_str)
cursor = conn.cursor()

cursor.execute("""
    Insert into ResumeInfo (Name, Phone, Email, LinkedIn, Programming_Language, Spoken_Language)
    VALUES (?, ?, ?, ?, ?, ?)
""", (probable_name, phone, email, linkedin, first_programming_lang, first_spoken_lang))
conn.commit()

print("Data successfully retrieved from S3 and loaded into SQL Server.")

# Move PDF to archive/
source_key = 'Dhivakar_Resume.pdf'
archive_key = 'archive/Dhivakar_Resume.pdf'

s3.copy_object(
    Bucket=bucket_name,
    CopySource={'Bucket': bucket_name, 'Key': source_key},
    Key=archive_key
)

s3.delete_object(Bucket=bucket_name, Key=source_key)

print(f"Moved PDF to archive folder in bucket '{bucket_name}'.")