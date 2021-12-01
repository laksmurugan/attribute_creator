import csv
import json
import requests
import time
import datetime
import copy

#read args

# file_name = from args
# environment = from args

"""
Config for making the POST call
"""

# prod
produserid = "ENTER_PROD_USER_ID"
produrl = "PROD_URL"
prodheader = {"authorization": "API_KEY",
              "Content-Type":"application/json",
              "User-Agent":"PostmanRuntime/7.24.1",
              "Connection":"keep-alive"
             }
# pre-prod
preproduserid = "ENTER_PREPROD_USER_ID"
preprodurl = "PREPROD_URL"
preprodheader = {"authorization": "API_KEY",
                 "Content-Type":"application/json",
                 "User-Agent":"PostmanRuntime/7.24.1",
                 "Connection":"keep-alive",
                }

"""
USER ENTRY BEGIN
"""
# Copy paste the value of the Cookie heder from any CM request

prodheader['Cookie'] = ''
preprodheader['Cookie'] = ''


# Enter File name
file_name='attribute_upload.csv'

"""
USER ENTRY COMPLETE
"""

def readcsvtolist(filename):
    at_list=[]
    with open(filename,encoding="utf-8-sig") as csvfile:
        csvfile = csv.reader(csvfile, delimiter=',')
        for row in csvfile:
            at_list.append(row)
    return at_list

request_payload_list=readcsvtolist(file_name)

def prepattributejson(filename, userid):
    attributes=readcsvtolist(filename)
    headers=attributes[0]
    attributes=attributes[1:]
    
    # initialization
    attribute_dict={}
    source_dict={}
    source_list=[]
    source_list.append(source_dict)
    attribute_dict["sources"]=source_list
    attribute_dict["channel"]="Web"
    attribute_dict["createdBy"]=userid
    attribute_dict["status"]="Published"
    source_dict["source"]="UPSV2Source"
    source_dict["priority"]="1"

    #mapping
    attributes_dict = {}
    master_attributes_dict = {}
    
    count=0
    for count in range(len(attributes)):
        newdict = {"attributeId":attributes[count][0],
                   "description":attributes[count][1],
                   "type":attributes[count][5],
                   "classification":attributes[count][6],
                   "refreshInterval":attributes[count][7],
                   "entityType":attributes[count][8],
                   "category":attributes[count][9]
                  }
        newdict.update(copy.deepcopy(attribute_dict))
        newdict["sources"][0]["profileName"]=attributes[count][2]
        newdict["sources"][0]["upsRequestPath"]=attributes[count][3]
        newdict["sources"][0]["upsResponsePath"]=attributes[count][4]
        master_attributes_dict[attributes[count][0]]=newdict

    return master_attributes_dict


#request_payload=prepattributejson(file_name,produserid)

# print(len(request_payload.keys()))


def getAttribute(attributename, url, header):
    try:
        r = requests.get(url+"/"+attributename, headers = header)
        r.raise_for_status()
        return json.loads(r.text)
    except requests.exceptions.HTTPError as err:
        raise SystemExit(err)


def createAttribute(item, url, header):
    try:
        r = requests.post(url, data = json.dumps(item), headers = header)
        r.raise_for_status()
    except requests.exceptions.HTTPError as err:
        raise SystemExit(err)


def createAllAttributes(request_payload, start_index, end_index, url, header):
    keys = list(request_payload.keys())[start_index:end_index+1]
    print("Creating {} attributes".format(len(keys)))
    for key in keys:
        print(datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'))
        print("Creating attribute: {}".format(key))
        createAttribute(request_payload[key],url, header)
        print("Successfully Created attribute: {}".format(key))
        print(datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'))
        time.sleep(5)
        print("Create complete")


def updateAttribute(item, url, header, userid):
    item["deprecated"]=False
    item["updatedBy"]=userid
    item["createdOn"]=getAttribute(item["attributeId"],url,header)["createdOn"]
    #print(json.dumps(item))
    try:
        r = requests.put(url+"/"+item["attributeId"], data = json.dumps(item), headers = header)
        r.raise_for_status()
    except requests.exceptions.HTTPError as err:
        raise SystemExit(err)


def updateAllAttributes(request_payload, start_index, end_index, url, header, userid):
    keys = list(request_payload.keys())[start_index:end_index+1]
    print("Updating {} attributes".format(len(keys)))
    for key in keys:
        print(datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'))
        print("Updating attribute: {}".format(key))
        updateAttribute(request_payload[key],url, header, userid)
        print("Successfully Updated attribute: {}".format(key))
        print(datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'))
        time.sleep(5)
    print("Update complete")

def main(env='preprod'):
	if env == 'prod':
		request_payload=prepattributejson(file_name,produserid)
		createAllAttributes(request_payload,0,len(request_payload.keys())-1, produrl, prodheader)
	else:
		request_payload=prepattributejson(file_name,preproduserid)
		createAllAttributes(request_payload,0,len(request_payload.keys())-1, preprodurl, preprodheader)

#main('prod')

main()