import fitz  # PyMuPDF
import os

# Open PDF
pdf_file = "refund_letter.pdf"
doc = fitz.open(pdf_file)

# Extract text from all pages
text = ""
for page in doc:
    text += page.get_text()

txt_file = os.path.splitext(pdf_file)[0] + ".txt"

# Write text to .txt file
with open(txt_file, "w", encoding="utf-8") as file:
    file.write(text)

print(f"Extracted text saved to: {txt_file}")
