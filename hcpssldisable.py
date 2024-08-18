import requests
from requests.packages.urllib3.exceptions import InsecureRequestWarning
import os

# Disable SSL warnings (equivalent to NoopHostnameVerifier in Java)
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

def search_and_download_object(hcp_url, object_guid, auth_code, save_path):
    url = f"{hcp_url}/rest/objects/{object_guid}"

    try:
        # Make a GET request to search and download the object
        response = requests.get(url, headers={'Authorization': auth_code}, verify=False)

        # Print the status code and reason for debugging purposes
        print(f"HTTP Status Code: {response.status_code}, Reason: {response.reason}")

        if response.status_code == 200:
            # Ensure that the content is not empty
            if response.content:
                print(f"Object {object_guid} found. Attempting to download.")
                
                # Ensure the directory exists
                os.makedirs(os.path.dirname(save_path), exist_ok=True)

                # Save the object to a file in chunks to avoid memory issues with large files
                with open(save_path, 'wb') as file:
                    for chunk in response.iter_content(chunk_size=8192):
                        if chunk:  # Filter out keep-alive new chunks
                            file.write(chunk)
                
                print(f"Object {object_guid} downloaded successfully to {save_path}.")
            else:
                print(f"Object {object_guid} was found, but the response content is empty.")
        elif response.status_code == 404:
            print(f"Object {object_guid} not found.")
        else:
            print(f"Failed to retrieve object {object_guid}. HTTP Status Code: {response.status_code}, Reason: {response.reason}")

    except requests.exceptions.RequestException as e:
        print(f"An error occurred while trying to download object {object_guid}: {e}")

# Example usage
hcp_url = "https://your-hcp-url"
auth_code = "Basic your_encoded_auth_code"  # Base64 encoded username:password
object_guid = "your-object-guid"

# Define the full path where you want to save the file
save_path = "/desired/path/to/save/downloaded_object_{object_guid}.dat"

# Search and download the object
search_and_download_object(hcp_url, object_guid, auth_code, save_path)
