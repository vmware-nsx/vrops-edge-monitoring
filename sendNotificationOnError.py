import requests
import time
import json
from config_reader import load_config

def current_milli_time():
    return round(time.time() * 1000)

def my_function(message, vROpsAccessToken):
    try:
        # Load configuration
        config, _ = load_config()  # We only need config, not credentials here
        
        # Get vrops details
        vrops_instance_ip = config['vrops_instance']['ip']
        vrops_instance_adapter_instance_id = config['vrops_instance']['adapter_instance_id']
        current_time_in_millis = current_milli_time()

        suite_api_json = {
            "eventType": "NOTIFICATION",
            "cancelTimeUTC": 0,
            "severity": "WARNING",
            "keyIndicator": False,
            "managedExternally": False,
            "resourceId": vrops_instance_adapter_instance_id,
            "message": message,
            "startTimeUTC": current_time_in_millis
        }

        suite_api_json_str = json.dumps(suite_api_json)
        print(suite_api_json_str)

        url = f'https://{vrops_instance_ip}/suite-api/api/events?_no_links=true'
        headers = {
            "Content-Type": "application/json",
            "accept": "*/*",
            "Authorization": f"vRealizeOpsToken {vROpsAccessToken}"
        }
        
        response = requests.post(
            url, 
            data=suite_api_json_str,
            headers=headers, 
            verify=False
        )
        
        if response.status_code == 200:
            print("Notification event sent successfully")
        else:
            print(f"Exception occurred to send notification to the target with error code: {response.status_code}")
            
    except Exception as e:
        print(f"Exception occurred while sending notification event: {str(e)}")