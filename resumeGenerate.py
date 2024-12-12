pip install Faker reportlab
import random
from faker import Faker
from reportlab.lib.pagesizes import letter
from reportlab.pdfgen import canvas

# List of skills, certifications, and courses
skills = [
    'Java', 'Python', 'JavaScript', 'React', 'Node.js', 'Spring Boot', 'SQL', 
    'MongoDB', 'Docker', 'Kubernetes', 'React Native', 'Flutter', 'GraphQL', 
    'AWS', 'Django', 'Vue.js', 'TensorFlow', 'CI/CD', 'Blockchain', 'Agile', 
    'Angular', 'TypeScript', 'Swift', 'Ruby on Rails', 'PHP', 'Go', 'Rust', 
    'Ruby', 'C++', 'C#', 'MATLAB', 'VHDL', 'Linux', 'Jenkins', 'Redis', 'Elasticsearch', 
    'Apache Kafka', 'Machine Learning', 'Deep Learning', 'Natural Language Processing', 
    'Computer Vision', 'Big Data', 'Data Engineering', 'Data Science', 'DevOps', 
    'Microservices', 'Serverless Architecture', 'OAuth', 'JWT', 'SQL Server', 'MySQL', 
    'PostgreSQL', 'Redis', 'Terraform', 'Ansible', 'Puppet', 'PowerShell', 'OpenCV'
]

certifications = [
    'AWS Certified Developer', 'Microsoft Certified: Azure Developer', 
    'Google Professional Cloud Architect', 'Certified Kubernetes Administrator', 
    'Google Cloud Certified', 'Certified Ethical Hacker', 
    'Certified Information Systems Security Professional (CISSP)', 
    'Oracle Certified Java Programmer', 'AWS Certified Solutions Architect', 
    'Microsoft Certified: Azure Solutions Architect', 'Certified ScrumMaster', 
    'Certified Cloud Security Professional (CCSP)', 'Certified Information Privacy Professional (CIPP)', 
    'Certified Big Data Professional (CBDP)', 'AWS Certified DevOps Engineer', 
    'Red Hat Certified Engineer (RHCE)', 'CompTIA Security+', 'CompTIA Network+', 
    'Certified Scrum Product Owner (CSPO)', 'Microsoft Certified: Power BI', 
    'Salesforce Certified Developer', 'Cisco Certified Network Associate (CCNA)', 
    'Certified Data Scientist', 'Certified Information Systems Auditor (CISA)', 
    'VMware Certified Professional (VCP)', 'Certified Artificial Intelligence Engineer', 
    'Certified Blockchain Developer', 'Professional Data Engineer by Google Cloud', 
    'SAP Certified Technology Associate', 'Certified Digital Marketing Professional', 
    'Python Institute PCPP – Python Certified Professional'
]

courses = [
    'Machine Learning by Coursera', 'Data Science Specialization by Johns Hopkins University', 
    'Full Stack Web Development by FreeCodeCamp', 
    'Deep Learning Specialization by Andrew Ng', 'AWS Solutions Architect by A Cloud Guru', 
    'JavaScript Algorithms and Data Structures by FreeCodeCamp', 
    'Data Structures and Algorithms by UC San Diego', 
    'CS50: Introduction to Computer Science by Harvard', 'Introduction to Data Science in Python by Coursera', 
    'Data Science with Python by IBM', 'Google IT Support Professional Certificate', 
    'Cloud Computing Specialization by University of Illinois', 'Blockchain Basics by University at Buffalo', 
    'Docker for Beginners by Udemy', 'AWS Certified Developer – Associate 2020 by Udemy', 
    'Complete Guide to TensorFlow for Deep Learning with Python by Udemy', 
    'Introduction to Artificial Intelligence by IBM', 'Intro to SQL by Khan Academy', 
    'Advanced SQL for Data Scientists by Coursera', 'Introduction to Big Data by UC Berkeley', 
    'Machine Learning with Python by Coursera', 'Full Stack Web Development with React by Coursera', 
    'Data Engineering with Google Cloud by Coursera', 'Python for Data Science by IBM', 
    'Microsoft Excel - Data Analysis with Excel by Coursera', 'Mobile App Development with React Native by Udemy', 
    'Mobile Development with Flutter by Coursera', 'Learning Path for Full Stack Developers by LinkedIn Learning', 
    'Agile Project Management by Coursera', 'Principles of Data Science by Coursera', 
    'Deep Learning for Computer Vision by Coursera', 'Certified Kubernetes Administrator (CKA) by Udemy', 
    'Introduction to Cyber Security by Coursera', 'Data Science Career Track by Springboard', 
    'DevOps Essentials by LinkedIn Learning', 'Machine Learning for Data Science and Analytics by FutureLearn',
    'Data Science and Machine Learning Bootcamp with R by Udemy', 'Cloud Computing with AWS by Coursera', 
    'Google Cloud Platform Fundamentals by Coursera', 'Advanced Data Science with Python by Coursera', 
    'Blockchain and Cryptocurrency Explained by Coursera', 'Intro to Machine Learning with PyTorch by Udacity', 
    'SQL for Data Science by Coursera'
]
# Function to generate a random resume
def generate_resume(file_name, skills, certifications, courses):
    fake = Faker()
    
    name = fake.name()
    address = fake.address().replace('\n', ', ')
    email = fake.email()
    phone = fake.phone_number()
    
    skill_set = random.sample(skills, k=5)  # Choose 5 random skills
    cert_set = random.sample(certifications, k=3)  # Choose 3 random certifications
    course_set = random.sample(courses, k=2)  # Choose 2 random courses
    
    c = canvas.Canvas(file_name, pagesize=letter)
    
    # Resume structure
    c.setFont("Helvetica-Bold", 16)
    c.drawString(100, 750, f"Resume: {name}")
    
    c.setFont("Helvetica", 12)
    c.drawString(100, 730, f"Email: {email}")
    c.drawString(100, 710, f"Phone: {phone}")
    c.drawString(100, 690, f"Address: {address}")
    
    c.setFont("Helvetica-Bold", 14)
    c.drawString(100, 660, "Skills:")
    
    c.setFont("Helvetica", 12)
    y_pos = 640
    for skill in skill_set:
        c.drawString(100, y_pos, f"- {skill}")
        y_pos -= 20

    c.setFont("Helvetica-Bold", 14)
    c.drawString(100, y_pos, "Certifications:")
    
    c.setFont("Helvetica", 12)
    y_pos -= 20
    for cert in cert_set:
        c.drawString(100, y_pos, f"- {cert}")
        y_pos -= 20

    c.setFont("Helvetica-Bold", 14)
    c.drawString(100, y_pos, "Courses:")
    
    c.setFont("Helvetica", 12)
    y_pos -= 20
    for course in course_set:
        c.drawString(100, y_pos, f"- {course}")
        y_pos -= 20

    c.save()

# Function to generate multiple PDFs
def generate_multiple_resumes(num_resumes, folder_path):
    for i in range(num_resumes):
        file_name = f"{folder_path}/resume_{i+1}.pdf"
        generate_resume(file_name, skills, certifications, courses)

# Example Usage:
generate_multiple_resumes(5, 'C:/Users/Chi/Documents')
