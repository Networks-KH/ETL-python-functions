# extract files from multiple sorces

import xml.etree.ElementTree as ET  # this module helps in processing XML files.
from datetime import datetime
from bs4 import BeautifulSoup
import html5lib
import requests
import pandas as pd
import glob
#set paths of files
tmpfile    = "C:\\.."     # file used to store all extracted data
logfile    = "logfile.txt"            # all event logs will be stored in this file
targetfile = "transformed_data.csv"   # file where transformed data is stored


# extract csv format function
def csv_file(file_name):
    dataframe = pd.read_csv(file_name)
    return dataframe

#extract json format function
def json_file(file_name):
    dataframe = pd.read_json(file_name)
    return dataframe

#extract xml format function
def extract_from_xml(file_to_process):
    dataframe = pd.DataFrame(columns=["name", "height", "weight"])
    tree = ET.parse(file_to_process)
    root = tree.getroot()
    for person in root:
        name = person.find("name").text
        height = float(person.find("height").text)
        weight = float(person.find("weight").text)
        dataframe = dataframe.append({"name":name, "height":height, "weight":weight}, ignore_index=True)
    return dataframe
extract_from_xml

 #extract function
def extract():
    extracted_data = pd.DataFrame(
        columns=['name', 'height', 'weight'])  # create an empty data frame to hold extracted data
    # process all csv files
    for csvfile in glob.glob("*.csv"):
        extracted_data = extracted_data.append(extract_from_csv(csvfile), ignore_index=True)
    # process all json files
    for jsonfile in glob.glob("*.json"):
        extracted_data = extracted_data.append(extract_from_json(jsonfile), ignore_index=True)
    # process all xml files
    for xmlfile in glob.glob("*.xml"):
        extracted_data = extracted_data.append(extract_from_xml(xmlfile), ignore_index=True)
    return extracted_data

#loding
def load(targetfile,data_to_load):
    data_to_load.to_csv(targetfile)

#loging
def log(message):
    timestamp_format = '%Y-%h-%d-%H:%M:%S' # Year-Monthname-Day-Hour-Minute-Second
    now = datetime.now() # get current timestamp
    timestamp = now.strftime(timestamp_format)
    with open("logfile.txt","a") as f:
        f.write(timestamp + ',' + message + '\n')

#runing ETL process
log("ETL Job Started")
log("Extract phase Started")
extracted_data = extract()
log("Extract phase Ended")
extracted_data
#requests library example "this library for extract from web"
url = "https://apilayer.com/account"
uplod = requests.get(url)
print(url)
print(uplod)
print(requests)
