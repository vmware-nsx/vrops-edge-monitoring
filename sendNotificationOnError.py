import requests
import requirements as req
import time
import json

def current_milli_time():
    return round(time.time() * 1000)
def my_function(message,vROpsAccessToken):
    try:
        #Get vrops details on/from which data is published/retrived
        vrops_instance = req.requirement['vrops_instance']
        vrops_instance_ip = vrops_instance['ip']
        vrops_instance_adapter_instance_id = vrops_instance['adapterInstanceId']
        current_time_in_millis = current_milli_time()

        suite_api_json={
        "eventType" : "NOTIFICATION",
        "cancelTimeUTC" : 0,
        "severity" : "WARNING",
        "keyIndicator" : False,
        "managedExternally" : False
        }

        suite_api_json["resourceId"] = vrops_instance_adapter_instance_id
        suite_api_json["message"] = message
        suite_api_json["startTimeUTC"] = current_time_in_millis

        suite_api_json = json.dumps(suite_api_json)
        print(suite_api_json)


        url = 'https://{}/suite-api/api/events?_no_links=true'.format(vrops_instance_ip)
        headers = {"Content-Type": "application/json","accept":"*/*","Authorization":"vRealizeOpsToken {}".format(vROpsAccessToken)}
        x = requests.post(url, data = suite_api_json,headers=headers, verify=False)
        if x.status_code == 200:
            print("Notification event sent successfully")
        else:
            print("Exception occured to send notification to the target with error code: " + str(x.status_code))
    except Exception as e:
        print("Exception occurred while sending notification event" + str(e))
