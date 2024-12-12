import os
from fpdf import FPDF

# Function to generate realistic bank contract PDFs
def generate_dummy_contract_pdfs(output_folder):
    contracts = [
        {
            "title": "Loan Agreement for Personal Loan",
            "loan_details": "This agreement covers a loan of $500,000 to be repaid over 15 years at an interest rate of 3.5%. The loan is provided by XYZ Bank, and the borrower is required to make monthly payments starting from the 1st of the following month after signing the agreement.",
            "payment_terms": "The borrower agrees to repay the loan amount in equal monthly installments. Payments will be made via automatic bank transfers to XYZ Bank's account. The first payment is due 30 days after the agreement signing.",
            "late_payment_terms": "Late payments will incur a penalty fee of 1.5% of the outstanding amount for every 30-day period after the due date. In case of non-payment for over 90 days, the loan may be considered in default, and the full amount will be due immediately.",
            "collateral_security": "As collateral for the loan, the borrower will pledge real estate property located at 123 Elm Street, which is valued at $600,000. The bank holds the right to seize the property in case of default on the loan.",
            "dispute_resolution": "Any disputes arising from this agreement will be settled through arbitration under the rules of the American Arbitration Association. The arbitration will take place in XYZ City."
        },
        {
            "title": "Mortgage Loan Agreement",
            "loan_details": "This mortgage agreement is for a loan amount of $1,000,000, with an interest rate of 2.9% for a 30-year fixed-rate term. The loan is for the purchase of residential property located at 456 Oak Street. The borrower agrees to make monthly mortgage payments to ABC Bank.",
            "payment_terms": "The loan will be repaid in monthly installments, with the first payment due 30 days after the signing of the agreement. The borrower has the option to make extra payments toward the principal without penalty.",
            "late_payment_terms": "In the event of late payment, a fee of 2% of the overdue amount will be assessed. If the payment is delayed by more than 60 days, the full loan balance will become due immediately, and the bank may initiate foreclosure proceedings.",
            "collateral_security": "The loan is secured by the residential property at 456 Oak Street. If the borrower defaults on the loan, the bank has the right to foreclose on the property and recover the loan balance.",
            "dispute_resolution": "Any disputes regarding this agreement will be resolved through mediation. If mediation fails, the matter will be taken to court in XYZ County."
        },
        {
            "title": "Business Loan Agreement",
            "loan_details": "This agreement grants a business loan of $200,000 to ABC Corporation, at an interest rate of 5% annually, to be repaid over 5 years. The loan will be used to expand operations and purchase equipment. Monthly payments are due on the 15th of each month.",
            "payment_terms": "The borrower agrees to repay the loan in monthly installments. The loan term is 60 months, and the borrower may request to refinance the loan after 36 months if necessary.",
            "late_payment_terms": "Late payments will incur a penalty of 2% of the overdue amount. If payments are more than 90 days late, the bank has the right to accelerate the loan and require immediate repayment of the remaining balance.",
            "collateral_security": "The loan is secured by the company's machinery and inventory, valued at $250,000. In case of default, the bank has the right to seize and liquidate the collateral.",
            "dispute_resolution": "Any disputes will be settled through binding arbitration in XYZ City, and the arbitration will be conducted in English."
        }
    ]
    
    # Create output folder for PDFs
    os.makedirs(output_folder, exist_ok=True)
    
    # Create PDF files for each contract
    for idx, contract in enumerate(contracts):
        pdf = FPDF()
        pdf.add_page()
        pdf.set_font("Arial", size=12)
        
        # Add contract title and content
        pdf.multi_cell(0, 10, contract["title"])
        pdf.ln(10)  # Add a line break
        
        pdf.multi_cell(0, 10, contract["loan_details"])
        pdf.ln(10)
        
        pdf.multi_cell(0, 10, contract["payment_terms"])
        pdf.ln(10)
        
        pdf.multi_cell(0, 10, contract["late_payment_terms"])
        pdf.ln(10)
        
        pdf.multi_cell(0, 10, contract["collateral_security"])
        pdf.ln(10)
        
        pdf.multi_cell(0, 10, contract["dispute_resolution"])
        
        # Save PDF file
        pdf.output(os.path.join(output_folder, f"contract_{idx+1}.pdf"))
    
    print("Realistic bank contract PDFs generated.")

# Define the output folder and generate PDFs
output_folder = "realistic_bank_contract_pdfs"
generate_dummy_contract_pdfs(output_folder)
