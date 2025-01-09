import requests
import xml.etree.ElementTree as ET
from config_reader import load_config

try:
    print("Requesting vRops Access Token")
    # Load configuration
    config, credentials = load_config()
    
    # Get vRops configuration
    vrops_instance_ip = config['vrops_instance']['ip']
    vrops_credentials = credentials['vrops_instance']

    url = f'https://{vrops_instance_ip}/suite-api/api/auth/token/acquire'
    payload = {
        'username': vrops_credentials['username'], 
        'password': vrops_credentials['password']
    }
    headers = {
        "Content-Type": "application/json",
        "accept": "application/json"
    }
    
    response = requests.post(url, json=payload, headers=headers, verify=False)
    response.raise_for_status()  # Raise an exception for bad status codes
    x = response.json()
    
    if x is not None:
        vROpsAccessToken = x['token']
    else:
        raise Exception("No token received from vRops")
        
except Exception as e:
    print(f"Exception occurred while fetching vRops Access Token: {str(e)}")
    raise  # Re-raise the exception to handle it in the calling code