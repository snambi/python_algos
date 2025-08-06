import requests
import json
import dotenv, os, hashlib
import aiohttp
from aiohttp import ClientSession 
import asyncio
from typing import Final

# Ideally, I would keep them under separate classes that can be passed around.
# for this exercise, having a global variable should be sufficient
HEADERS = {}
AIRTABLE_BASE_ID = ""
AIRTABLE_API_TOKEN = ""

def init():

    dotenv.load_dotenv()
    
    global AIRTABLE_API_TOKEN, AIRTABLE_BASE_ID
    
    AIRTABLE_API_TOKEN = os.getenv("AIRTABLE_API_TOKEN")
    AIRTABLE_BASE_ID = os.getenv("AIRTABLE_BASE_ID")
    
    if AIRTABLE_API_TOKEN is None or AIRTABLE_BASE_ID is None:
        raise Exception(f"AIRTABLE_BASE_ID or AIRTABLE_API_TOKEN is not set. please make sure the .env file contains these values")

    global HEADERS
    HEADERS = {
        "Authorization": f"Bearer {AIRTABLE_API_TOKEN}",
        "Content-Type": "application/json"
    }


def fetch_data_from_airtable(table_name):
    url = f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{table_name}"
    response = requests.get(url, headers=HEADERS)
    response.raise_for_status()
    records = response.json().get("records", [])
    # Extract just the fields
    return [record["fields"] for record in records]


async def  update_compressed_json(recordId:str, data:dict, max_retries=3) -> bool:
    
    url = f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/Applicants/{recordId}"

    compressed_json = json.dumps(data, separators=(',', ':'))
    sha256_hash = hashlib.sha256(compressed_json.encode('utf-8')).hexdigest()
    
    payload = {
        "fields": {
            "Compressed_JSON": compressed_json,
            "Shortlist_status": "Pending",
            "SHA": sha256_hash
        }
    }

    async with aiohttp.ClientSession() as session:
        for attempt in range( 1, max_retries+1 ):
            try:
                async with session.patch(url, headers=HEADERS, json=payload) as response:
                    if response.status == 200:
                        #print(f"record {recordId} updated successfully")
                        return True
                    else:
                        body = await response.text()
                        print(f"Attempt {attempt}: Failed ({response.status}) - {body}")
            except Exception as e:
                print(f"Attempt {attempt} failed with {e}")
                
            wait_time = 2 ** attempt
            print(f"Retrying in {wait_time} seconds...")
            await asyncio.sleep(wait_time)
            
        return False

        


PERSONAL_KEYS:Final = ["Full Name", "Email", "Location", "LinkedIn"]
EXPERIENCE_KEYS:Final = ["Company", "Title", "Start", "End", "Technologies"]
SALARY_KEYS:Final = ["Preferred Rate","Minimum_Rate", "Currency", "Availability"]

def combine_data(personal_data:list, experience_data:list, salary_data:list ) -> dict:
    data = {}
    
    for x in personal_data:
        if "Email" in x :
            #print(f"personal = {x}")
            filtered = {k: x[k] for k in PERSONAL_KEYS if k in x}
            applicant = x["Applicants"][0]
            data[applicant] = { "personal": filtered } 
    
    for x in experience_data:
        if "Company" in x:
            #print(f"experience = {x}")
            filtered = { k:x[k] for k in EXPERIENCE_KEYS if k in x } 
            applicant = x["Applicants"][0]
            if "experience" not in data[applicant]:
                y = data[applicant] 
                y["experience"] = [filtered]
            else:
                y = data[applicant]["experience"]
                y.append(filtered)
    
    for x in salary_data:
        if "Preferred Rate" in x:
            #print(f"salary = {x}")
            filtered = { k:x[k] for k in SALARY_KEYS if k in x}
            applicant = x["Applicants"][0]
            y = data[applicant]
            y["salary"] = filtered
    
    return data

async def main():
    """_summary_
    Following three things are done by the script,
    1. Fetch the data from Salary, Personal and Experience tables
    2. Organize the data as per Applicant ID
    3. Created a compressed JSON and update the Applicants table 
    """
    
    # Fetch data
    personal_data = fetch_data_from_airtable("Personal_Details")
    experience_data = fetch_data_from_airtable("Work_Experience")
    salary_data = fetch_data_from_airtable("Salary_Prefs")

    # Combine data
    combined_data = combine_data(personal_data,experience_data, salary_data)
    
    # Update Applicants with Compressed_JSON
    for x in combined_data.keys():
        result = await update_compressed_json(x ,combined_data[x])
        if result == False:
            print(f"update to {x} failed")
        else:
            print(f"update to {x} Succeeded")
    
##
## To run the script it equires a .env file with following two values
## AIRTABLE_API_TOKEN , AIRTABLE_BASE_ID 
##
if __name__ == "__main__":
    init()
    asyncio.run(main())