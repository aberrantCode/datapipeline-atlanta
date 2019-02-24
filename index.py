#!/usr/bin/env python3.7
import requests
import os
import datetime
from google.cloud import storage
from bs4 import BeautifulSoup

#runtime parameters/variables
bucket_name = 'police-data'
bucket_folder = 'atlanta'

#push the payload into cloud storage
def saveData(data):
    print('Saving data')
    print(' -> Opening bucket: ' + bucket_name)
    client = storage.Client()
    bucket = client.get_bucket(bucket_name)
    blobName = bucket_folder + "/" + datetime.datetime.today().strftime('%Y%m%d%H%M%S')+'.csv'
    print(' -> Creating blob: ' + blobName)
    blob = bucket.blob(blobName)
    print(' -> Uploading blob content')
    blob.upload_from_string(data, content_type='text/plan')
    print(' -> Blob Saved with '+str(len(data.splitlines()))+' rows')

#simulate a user selecting city wide current month data
def getData():
    print('Requesting data')
    post_url = "http://opendataportal.azurewebsites.us/Crimedata/Default.aspx"
    headers = requests.utils.default_headers()
    headers.update({
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8', 
        'Accept-Encoding': 'gzip, deflate', 
        'Accept-Language': 'en-US,en;q=0.9', 
        'Cache-Control': 'max-age=0', 
        'Content-Type': 'application/x-www-form-urlencoded',
        'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:52.0) Gecko/20100101 Firefox/52.0' 
    })
    #first request will get viewstate
    print(' -> Downloading form')
    response = requests.get(post_url, headers=headers)
    if response.status_code == 200:
        print(' -> Success')
    else:
        print(' -> Failure Code: '+str(response.status_code))
        return None
    
    #parse results and extract the viewstate metadata to be pushed into next request
    bs = BeautifulSoup(response.content,features="html.parser")
    viewstate = bs.find("input", {"id": "__VIEWSTATE"}).attrs['value']
    viewstategen = bs.find("input", {"id": "__VIEWSTATEGENERATOR"}).attrs['value']
    eventvalidation = bs.find("input", {"id": "__EVENTVALIDATION"}).attrs['value']
    #select the radio option for city wide crime
    form_values = { 
        '__EVENTTARGET': 'ctl00$MainContent$rblArea$0', 
        '__EVENTARGUMENT': None, 
        '__LASTFOCUS': None, 
        '__VIEWSTATE': viewstate, 
        '__VIEWSTATEGENERATOR': viewstategen, 
        '__EVENTVALIDATION': eventvalidation, 
        'ctl00$MainContent$rblArea': 'CityWide' 
    }
    print(' -> Selecting City Wide Crime')
    response = requests.post(post_url, headers=headers, data=form_values)
    if response.status_code == 200:
        print(' -> Success')
    else:
        print(' -> Failure Code: '+str(response.status_code))
        return None    
    #extract the view state from the form
    bs = BeautifulSoup(response.text,features="html.parser")
    viewstate = bs.find("input", {"id": "__VIEWSTATE"}).attrs['value']
    viewstategen = bs.find("input", {"id": "__VIEWSTATEGENERATOR"}).attrs['value']
    eventvalidation = bs.find("input", {"id": "__EVENTVALIDATION"}).attrs['value']
    #format month/year selections for form submission
    month_str = datetime.date.today().strftime('%#m' if os.name == 'nt' else '%-#m')
    year_str = datetime.date.today().strftime('%Y')
    #post the form selections
    form_values = { 
        '__EVENTTARGET': 'ctl00$MainContent$rblArea$0', 
        '__EVENTARGUMENT': None, 
        '__LASTFOCUS': None, 
        '__VIEWSTATE': viewstate, 
        '__VIEWSTATEGENERATOR': viewstategen, 
        '__EVENTVALIDATION': eventvalidation, 
        'ctl00$MainContent$rblArea': 'CityWide', 
        'ctl00$MainContent$ddlMonth': month_str, 
        'ctl00$MainContent$ddlYear': year_str, 
        'ctl00$MainContent$ddlCrimeType': 'AllCrime', 
        'ctl00$MainContent$btnSearch': 'Search' 
    }
    print(' -> Searching Month/Year: '+ month_str + '/' + year_str)
    response = requests.post(post_url,headers=headers, data=form_values)
    if response.status_code == 200:
        print(' -> Success')
    else:
        print(' -> Failure Code: '+str(response.status_code))
        return None
    #extract viewstate from the form and maintain form values
    bs = BeautifulSoup(response.text,features="html.parser")
    viewstate = bs.find("input", {"id": "__VIEWSTATE"}).attrs['value']
    viewstategen = bs.find("input", {"id": "__VIEWSTATEGENERATOR"}).attrs['value']
    eventvalidation = bs.find("input", {"id": "__EVENTVALIDATION"}).attrs['value']
    form_values = {
        'ctl00$MainContent$rblArea': 'CityWide', 
        'ctl00$MainContent$ddlMonth': month_str,
        'ctl00$MainContent$ddlYear': year_str, 
        'ctl00$MainContent$ddlCrimeType': 'AllCrime', 
        'ctl00$MainContent$btnDownload': 'Download CSV',
        '__VIEWSTATE': viewstate,
        '__VIEWSTATEGENERATOR': viewstategen,
        '__EVENTVALIDATION': eventvalidation
    }
    #download the results as plain text or return nothing
    response = requests.post(post_url,headers=headers, data=form_values)
    if response.status_code == 200:
        print(' -> Success')
        return response.text
    else:
        print(' -> Failure Code: '+str(response.status_code))
        return None

def main():
    print("Getting Data")
    data = getData()
    if data is None:
        print("Error getting data")
    else:
        saveData(data)

#use local credentials if present
credFile = os.getcwd() + "\\credentials.json"
if os.path.isfile(credFile):
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = credFile

main()