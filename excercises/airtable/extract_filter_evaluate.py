from typing import Tuple,Dict, Final
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
        return (applicant_id, f"❌ Error: {str(e)}")
    
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




def update(analyzed:dict, rejected:dict):
    pass
    # 


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
    
    update(analyzed, rejected)
    
    

if __name__ == "__main__":
    init()
    asyncio.run(main())