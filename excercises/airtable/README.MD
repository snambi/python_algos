# Airtable Multi-Table Form + JSON processing

# Setup

```bash
git clone https://github.com/snambi/python_algos.git
cd pyton_algos
uv venv
. .venv/bin/activate
uv build
```

# Create .env file

```bash
# this file should be present in the root of the project
cat .env
AIRTABLE_API_TOKEN="patzp78NKoL8VTsXV.0646if5a9b57cccf94c1cdf3b694e2873b3578f914a545pf4fd77f1a35b12b26"
AIRTABLE_BASE_ID="appPPigONKjHDbhzb"
GEMINI_API_KEY="AIzaSyUBg_4Tr8zYJFHzxxWvUDLi_JW9ZRAM4tc"
```

# Execute Scripts

```bash
❯ python excercises/airtable/airtable_json_update.py
update to rec99C2vxYyujBhQ2 Succeeded
update to recFa7ArKd44BLG2I Succeeded
update to recnDhYuu7aLTbtQS Succeeded
update to recjyT71giVsldz06 Succeeded

❯ python excercises/airtable/extract_filter_evaluate.py
data extracted
applicants filtered based on criteria
llm analysis completed 
record recFa7ArKd44BLG2I updated successfully
record recjyT71giVsldz06 updated successfully
record recnDhYuu7aLTbtQS updated successfully
record rec99C2vxYyujBhQ2 updated successfully
Upserting lead for Application_ID: 2
Upserting lead for Application_ID: 1
Upserting lead for Application_ID: 4
tables updated 
```
