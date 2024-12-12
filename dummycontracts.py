import os
from fpdf import FPDF

# Function to generate dummy bank contract PDFs
def generate_dummy_contract_pdfs(output_folder):
    contracts = [
        "Contract 1: This agreement covers the terms for a loan of $500,000 with an interest rate of 3.5% for 15 years. The loan is provided by XYZ Bank.",
        "Contract 2: The agreement includes a loan amount of $1,000,000 for a 30-year period, with a fixed interest rate of 2.9%. The loan is a mortgage loan with monthly repayments.",
        "Contract 3: This agreement pertains to a business loan of $200,000, with a 5% annual interest rate. The loan term is for 5 years, with quarterly repayments.",
        "Contract 4: This agreement involves a student loan of $100,000 at an interest rate of 4.5%. The loan term is 10 years, with monthly repayments.",
        "Contract 5: The agreement describes a car loan of $30,000 at 7% interest rate for a 7-year period. The loan is secured against the vehicle."
    ]
    
    # Create output folder for PDFs
    os.makedirs(output_folder, exist_ok=True)
    
    # Create PDF files for each contract
    for idx, contract in enumerate(contracts):
        pdf = FPDF()
        pdf.add_page()
        pdf.set_font("Arial", size=12)
        pdf.multi_cell(0, 10, contract)
        pdf.output(os.path.join(output_folder, f"contract_{idx+1}.pdf"))
    
    print("Dummy contract PDFs generated.")

# Define the output folder and generate PDFs
output_folder = "dummy_contract_pdfs"
generate_dummy_contract_pdfs(output_folder)
