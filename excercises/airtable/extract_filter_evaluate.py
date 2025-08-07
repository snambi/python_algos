import hashlib
from typing import Tuple,Dict, Final, Optional

import aiohttp
import airtable_json_update
import requests
from datetime import datetime
import asyncio
import json, os
from google import genai
from google.genai.client import Client


client:Client

def init():
    airtable_json_update.init()

    GEMINI_API_KEY =os.getenv("GEMINI_API_KEY")
    global client
    client = genai.Client(api_key=GEMINI_API_KEY)
    

def extract():
    url = f"https://api.airtable.com/v0/{airtable_json_update.AIRTABLE_BASE_ID}/Applicants"
    response = requests.get(url, headers=airtable_json_update.HEADERS)
    response.raise_for_status()
    records = response.json().get("records", [])
    # Extract just the fields
    data = {}
    for record in records:
        recordId = record["id"]
        fields = record["fields"]
        data[recordId] = fields
        
    # extract only Compressed JSON
    result:dict = {}
    
    for x in data:
        d = data[x]
        if "Compressed_JSON" in d:
            k = d["Application_ID"]
            v = d["Compressed_JSON"]
            j = {"Application_ID": k, "Compressed_JSON": v}
            result[x] = j
            
    return result

# Define constants
TIER_1_COMPANIES:Final = {"Google", "Meta", "OpenAI"}
ALLOWED_LOCATIONS:Final = {"US", "Canada", "UK", "Germany", "India"}
MAX_RATE:Final = 100
MIN_AVAILABILITY:Final = 20

def calculate_experience_years(experience_list):
    total_days = 0
    for job in experience_list:
        try:
            start = datetime.strptime(job["Start"], "%Y-%m-%d")
            end = datetime.strptime(job["End"], "%Y-%m-%d")
            total_days += (end - start).days
        except:
            continue
    return total_days / 365.0  # Convert to years

def is_tier_1_company(experience_list):
    return any(job["Company"] in TIER_1_COMPANIES for job in experience_list)


def is_valid_applicant(input):
    
    result:str = "Applicant has "
    
    record = eval(input.get("Compressed_JSON"))
    # Experience
    experience = record.get("experience", [])
    years = calculate_experience_years(experience)
    has_tier1 = is_tier_1_company(experience)
    experience_ok = years >= 4 or has_tier1
    
    if years >= 4 and has_tier1:
        result += "more than 4 years of experience and worked in tier1 company."
    else:
        if years >= 4:
            result += "more than 4 years of experience."
        if has_tier1:
            result += "has tier 1 company experience."
            
    # Compensation
    salary = record.get("salary", {})
    rate_ok = salary.get("Preferred Rate", float('inf')) <= MAX_RATE
    
    if rate_ok:
        result += f" Rate is less than {MAX_RATE}."
        
    availability_ok = salary.get("Availability", 0) >= MIN_AVAILABILITY
    
    if availability_ok:
        result += f" Availability is greater than {MIN_AVAILABILITY} hours/week"

    # Location
    location = record.get("personal", {}).get("Location", "")
    location_ok = location in ALLOWED_LOCATIONS


    valid = experience_ok and rate_ok and availability_ok and location_ok
    
    return valid, result


def filter(data:dict) -> Tuple[Dict, Dict]:
    filtered = {}
    rejected = {}
    
    for k in data.keys():
        v,s = is_valid_applicant( data[k])
        if v :
            #print(f"s = {s}")
            temp =  {"compressed": data[k], "score_reason": s}
            filtered[k] = temp
        else:
            rejected[k] = data[k]
            
    return filtered, rejected
        

def format_prompt(applicant_data: dict) -> str:
    return f"""
            You are a recruiting analyst. Given this JSON applicant profile, do four things:
            1. Provide a concise 75-word summary.
            2. Rate overall candidate quality from 1-10 (higher is better).
            3. List any data gaps or inconsistencies you notice.
            4. Suggest up to three follow-up questions to clarify gaps.

            Return exactly:
            Summary: <text>
            Score: <integer>
            Issues: <comma-separated list or 'None'>
            Follow-Ups: <bullet list>
            
            Respond ONLY with a **valid JSON object**.
            Do NOT include markdown formatting like ```json.
            Do NOT include any explanations or extra text.

            Applicant JSON:
            {json.dumps(applicant_data, indent=2)}
            """

async def analyze_applicant(applicant_id: int, applicant_data: dict) -> tuple:
    prompt = format_prompt(applicant_data)
    try:
        response = client.models.generate_content(model="gemini-2.5-flash", contents=prompt)
        return (applicant_id, response.text)
    except Exception as e:
        return (applicant_id, f"Error: {str(e)}")
    
    
async def analyze(data:dict) -> dict:
    for x in data.keys():
        rec = data[x]
        if "compressed" in rec:
            y = data[x]
            text = await analyze_applicant(x, y["compressed"])
           
            parsed = json.loads(text[1])
            # print(f"llm output: {parsed}")
            y["llm_analysis"] = parsed
    

    return data


async def update_applicant(status:str, 
                           recordId:str,
                     llm_summary:str|None=None, 
                     llm_score:int=0, 
                     followups:str|None=None, max_retries=3 ):
    
    url = f"https://api.airtable.com/v0/{airtable_json_update.AIRTABLE_BASE_ID}/Applicants/{recordId}"
    
    payload = {
        "fields": {
            "Shortlist_status": status,
        }
    }
    
    if llm_summary is not None:
        payload["fields"]["LLM_Summary"] = llm_summary

    if followups is not None:
        payload["fields"]["Follow_Ups"] = followups

    # Add score only if it's meaningful (optional)
    if llm_score > 0:
        payload["fields"]["LLM_Score"] = llm_score # type: ignore
    
    async with aiohttp.ClientSession() as session:
        for attempt in range( 1, max_retries+1 ):
            try:
                async with session.patch(url, headers=airtable_json_update.HEADERS, json=payload) as response:
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

def compare(v1:str, v2:str):
    
    sha1  = hashlib.sha256(v1.encode('utf-8')).hexdigest()
    sha2  = hashlib.sha256(v2.encode('utf-8')).hexdigest()
    
    if sha1 == sha2 :
        return True
    else:
        return False
    
async def upsert_leads(recordId:str, data:dict):
    base_id = airtable_json_update.AIRTABLE_BASE_ID
    headers = airtable_json_update.HEADERS

    application_id = data["compressed"]["Application_ID"]

    print(f"Upserting lead for Application_ID: {application_id}")

    # Step 1: Search for existing lead by ApplicationId (properly quoted)
    search_url = f"https://api.airtable.com/v0/{base_id}/Leads"
    params = {
        "filterByFormula": f"{{Applicants}} = {application_id}",
        "maxRecords": 1
    }

    search_response = requests.get(search_url, headers=headers, params=params)
    search_response.raise_for_status()
    results = search_response.json()

    if results.get("records"):
        
        # before updating check whether the data changed
        fields = results["records"][0]["fields"]
        v2 = json.dumps(data["compressed"], separators=(',', ':'))
        isSame = compare(fields["Compressed_JSON"], v2 )
        if isSame :
            return True
        
        # Step 2: Lead exists — update it
        lead_id = results["records"][0]["id"]
        
        update_url = f"https://api.airtable.com/v0/{base_id}/Leads/{lead_id}"

        patch_payload = {
            "fields": {
                "Applicants": [recordId],  # if this is a linked field
                "Compressed_JSON": json.dumps(data["compressed"], separators=(',', ':')),
                "Score_Reason": data["score_reason"]
            }
        }

        patch_response = requests.patch(update_url, headers=headers, json=patch_payload)
        patch_response.raise_for_status()
        print(f"Updated existing Lead: {lead_id}")
        return patch_response.json()

    else:
        # Step 3: No existing record — create a new one
        create_url = f"https://api.airtable.com/v0/{base_id}/Leads"

        post_payload = {
            "fields": {
                "Applicants": [recordId],  # if this is a linked field
                "Compressed_JSON": json.dumps(data["compressed"], separators=(',', ':')),
                "Score_Reason": data["score_reason"]
            }
        }

        post_response = requests.post(create_url, headers=headers, json=post_payload)
        post_response.raise_for_status()
        print("Created new Lead")
        return post_response.json()


    

async def update(analyzed:dict, rejected:dict):
    
    for x in analyzed:
        data = analyzed[x]["llm_analysis"]
        #print( f"data {data}")
        followups = ", ".join(data["Follow-Ups"])
        res = await update_applicant(status="Shortlisted",recordId=x, llm_score=data["Score"], llm_summary=data["Summary"], followups=followups)
        if res == True:
            print(f"record {x} updated successfully")
        else:
            print(f"record {x} not updated successfully")
            
    for x in rejected:
        res = await update_applicant(status="Rejected", recordId=x)
        if res == True:
            print(f"record {x} updated successfully")
        else:
            print(f"record {x} not updated successfully")
            
    for x in analyzed:
        await upsert_leads(recordId=x, data=analyzed[x])
    


async def main():
    """_summary_
    This function does the following actions
    1. extract - extract compressed JSON from Applicants table
    2. filter - apply the selection criteria to the data and filters out the data
    3. analyse - analyse the data against gemini LLM 
    4. update - update the leads table with the results
    """
    extracted_data = extract()
    # print(f"data {extracted_data}")
    
    filtered, rejected= filter(extracted_data)
    # print(f"filtered {filtered}")
    
    analyzed = await analyze(filtered)
    print(f"analyzed {analyzed}")
    
    await update(analyzed, rejected)
    
    

if __name__ == "__main__":
    init()
    asyncio.run(main())