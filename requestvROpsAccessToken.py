import requests
import xml.etree.ElementTree as ET
import requirements as req

try:
    print("Requesting vRops Access Token")
    vrops_instance = req.requirement['vrops_instance']
    vrops_instance_ip = vrops_instance['ip']
    vrops_instance_cred = vrops_instance['credential']

    url = 'https://{}/suite-api/api/auth/token/acquire'.format(vrops_instance_ip)
    payload = {'username': vrops_instance_cred['username'], 'password':vrops_instance_cred['password']}
    headers = {"Content-Type": "application/json","accept":"application/json"}
    x = requests.post(url, json = payload,headers=headers,verify=False).json()
    if x != None :
         vROpsAccessToken =x['token']
except Exception as e:
    print("Exception occured while fetching vRops Access Token " + str(e))