import requests
import ssl

def get_verified_session(cert_chain_path):
    context = ssl.create_default_context(cafile=cert_chain_path)
    session = requests.Session()
    session.mount('https://', HTTPAdapter(max_retries=3, ssl_context=context))
    return session

# Example usage:
cert_chain_file = '/path/to/your/cert/chain.pem'
session = get_verified_session(cert_chain_file)
response = session.get('https://your_hitachi_content_platform_endpoint')
