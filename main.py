import os
import time
import requests
import psycopg2
import pandas as pd
from jobspy import scrape_jobs
from dotenv import load_dotenv
from bs4 import BeautifulSoup

# LOAD FIRST
load_dotenv()

# --- CONFIGURATION ---
DB_URL = os.environ.get("DATABASE_URL")
BOT_TOKEN = os.environ.get("TELEGRAM_TOKEN")
CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID")

def get_db_connection():
    return psycopg2.connect(DB_URL)

def init_db():
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS seen_jobs (
            job_id TEXT PRIMARY KEY,
            company TEXT,
            title TEXT,
            site TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)
    conn.commit()
    cur.close()
    conn.close()

def send_telegram_alert(job):
    # Ensure this handles the dictionary format we pass to it
    site = job.get('site', 'Unknown')
    title = job.get('title', 'No Title')
    company = job.get('company', 'No Company')
    url = job.get('job_url', '#')
    
    msg = (
        f"🇸🇬 **NEW INTERNSHIP ({site})**\n\n"
        f"🏢 **{company}**\n"
        f"👨‍💻 {title}\n"
        f"🔗 [Apply Here]({url})"
    )
    
    tg_url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    payload = {"chat_id": CHAT_ID, "text": msg, "parse_mode": "Markdown"}
    
    # # --- NEW DEBUGGING LOGIC ---
    # response = requests.post(tg_url, json=payload)
    # if response.status_code != 200:
    #     print(f"❌ TELEGRAM API ERROR: {response.status_code}")
    #     print(f"❌ REASON: {response.text}")
    # else:
    #     print("✅ Message successfully sent to Telegram!")
    # ---------------------------

def is_target_role(title: str) -> bool:
    """
    Returns True if the job title matches your career targets (SWE/Quant).
    Returns False for irrelevant roles (HR, Sales, Marketing).
    """
    title_lower = title.lower()
    
    # 1. Reject conditions (The "Blacklist")
    # If any of these words are in the title, kill it immediately.
    blacklist = ["sales", "marketing", "hr", "human resources", "accounting", "civil", "mechanical", "electrical"]
    if any(bad_word in title_lower for bad_word in blacklist):
        return False
        
    # 2. Accept conditions (The "Whitelist")
    # If the title passed the blacklist, check if it has a tech/quant keyword.
    whitelist = [
        "software", "swe", "developer", "programmer", 
        "quant", "trading", "trader", "algorithmic", "researcher", 
        "data", "ai", "machine learning", "ml", "backend", "frontend", "fullstack"
    ]
    return any(good_word in title_lower for good_word in whitelist)

def run_pipeline():
    print("🚀 Running Broad Catch-All Pipeline for SG...")
    
    # # --- QUICK TEST ---
    # send_telegram_alert({
    #     "site": "System",
    #     "title": "Cloud Pipeline Woke Up",
    #     "company": "GitHub Actions",
    #     "job_url": "https://github.com"
    # })
    # # ---------------------------

    # 1. Make ONE broad request using Boolean logic
    broad_search = "(software OR developer OR data OR quant OR AI OR machine learning OR engineer) AND intern"
    
    try:
        # Increase results_wanted slightly because we expect to throw some away
        jobs = scrape_jobs(
            site_name=["linkedin", "indeed", "glassdoor"],
            search_term=broad_search,
            location="Singapore",
            results_wanted=30, 
            hours_old=24,
            country_indeed='Singapore'
        )
        
        if jobs is None or jobs.empty:
            print("⚠️ No results found.")
            return

        conn = get_db_connection()
        cur = conn.cursor()

        # 2. Apply the Local Filter Engine
        for index, row in jobs.iterrows():
            title = row['title']
            company = row['company']
            
            # Pass the title through your custom bouncer function
            if not is_target_role(title):
                print(f"🗑️ Filtered out non-target role: {title} at {company}")
                continue # Skip to the next job
                
            # If it survives the filter, process it normally
            raw_id = str(row['id'])
            site = row['site']
            unique_id = f"{site}_{raw_id}"
            
            cur.execute("SELECT job_id FROM seen_jobs WHERE job_id = %s", (unique_id,))
            if cur.fetchone() is None:
                print(f"✨ New CS/Quant Alert: {title} at {company}")
                send_telegram_alert(row.to_dict())
                
                cur.execute(
                    "INSERT INTO seen_jobs (job_id, company, title, site) VALUES (%s, %s, %s, %s)",
                    (unique_id, company, title, site)
                )
                conn.commit()
                
    except Exception as e:
        print(f"❌ Pipeline Error: {e}")
    finally:
        cur.close()
        conn.close()

def scrape_internsg_pipeline():
    print("🚀 Running InternSG Pipeline...")
    
    # We target the IT category specifically to reduce initial noise
    target_url = "https://www.internsg.com/jobs/?f_0=1&f_p=107&f_i=61&filter_s="
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }
    
    try:
        response = requests.get(target_url, headers=headers)
        if response.status_code != 200:
            print(f"❌ InternSG Error: {response.status_code}")
            return
            
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Find all anchor tags that link to a specific job post
        job_links = soup.find_all('a', href=True)
        
        conn = get_db_connection()
        cur = conn.cursor()
        
        for link in job_links:
            url = link['href']
            title = link.get_text(strip=True)
            
            # 1. Basic validation: Is it actually a job link?
            if '/job/' in url and len(title) > 5:
                
                # 2. Pass it through your existing filter!
                if not is_target_role(title):
                    continue
                
                # Create a unique ID from the URL (e.g., extracts 'software-engineer-intern')
                raw_id = url.strip('/').split('/')[-1]
                unique_id = f"internsg_{raw_id}"
                
                cur.execute("SELECT job_id FROM seen_jobs WHERE job_id = %s", (unique_id,))
                if cur.fetchone() is None:
                    print(f"✨ New InternSG Alert: {title}")
                    
                    # Package it for your existing Telegram function
                    job_data = {
                        "site": "InternSG",
                        "title": title,
                        "company": "View Listing for Details", # InternSG HTML makes company hard to cleanly extract without complex selectors
                        "job_url": url
                    }
                    send_telegram_alert(job_data)
                    
                    cur.execute(
                        "INSERT INTO seen_jobs (job_id, company, title, site) VALUES (%s, %s, %s, %s)",
                        (unique_id, "InternSG", title, "internsg")
                    )
                    conn.commit()
                    
    except Exception as e:
        print(f"❌ InternSG Pipeline Error: {e}")
    finally:
        # Check if cur and conn exist before closing in case the error happened before they were defined
        if 'cur' in locals(): cur.close()
        if 'conn' in locals(): conn.close()

def scrape_greenhouse_pipeline():
    print("🚀 Running Greenhouse ATS Pipeline...")
    
    # You just maintain a hardcoded list of company board tokens
    greenhouse_tokens = [
    "stripe",                 # Stripe
    "optiver",                # Optiver
    "hudsonrivertrading",     # Hudson River Trading (HRT)
    "towerresearchcapital",   # Tower Research Capital
    "citadel",                # Citadel
    "citadelsecurities",      # Citadel Securities
    "coinbase",               # Coinbase
    "motional",               # Motional
    "twilio",                 # Twilio
    "zendesk"                 # Zendesk
]
    
    conn = get_db_connection()
    cur = conn.cursor()
    
    for token in greenhouse_tokens:
        url = f"https://boards-api.greenhouse.io/v1/boards/{token}/jobs"
        
        try:
            response = requests.get(url)
            # If a token is wrong or the company doesn't use Greenhouse, skip it quietly
            if response.status_code != 200:
                print(f"⚠️ Could not fetch data for {token}. Check if the token is correct.")
                continue
                
            jobs_data = response.json().get("jobs", [])
            
            for job in jobs_data:
                title = job.get("title", "")
                
                # 1. Pass the job through your existing filter!
                if not is_target_role(title):
                    continue
                
                # Greenhouse provides the direct apply URL and a unique internal ID natively
                job_url = job.get("absolute_url")
                raw_id = str(job.get("id"))
                unique_id = f"greenhouse_{raw_id}"
                
                # 2. Database Deduplication
                cur.execute("SELECT job_id FROM seen_jobs WHERE job_id = %s", (unique_id,))
                if cur.fetchone() is None:
                    print(f"✨ New High-Speed Alert: {title} at {token.capitalize()}")
                    
                    job_data = {
                        "site": "Greenhouse API",
                        "title": title,
                        "company": token.capitalize(),
                        "job_url": job_url
                    }
                    send_telegram_alert(job_data)
                    
                    cur.execute(
                        "INSERT INTO seen_jobs (job_id, company, title, site) VALUES (%s, %s, %s, %s)",
                        (unique_id, token.capitalize(), title, "greenhouse")
                    )
                    conn.commit()
                    
        except Exception as e:
            print(f"❌ Error scraping Greenhouse for {token}: {e}")
            
    if 'cur' in locals(): cur.close()
    if 'conn' in locals(): conn.close()

if __name__ == "__main__":
    init_db()
    
    run_pipeline()              # Engine 1: Broad JobSpy (LinkedIn/Indeed)
    scrape_internsg_pipeline()  # Engine 2: InternSG HTML 
    scrape_greenhouse_pipeline()# Engine 3: Target Company JSON APIs
    
    print("✅ All data pipelines complete.")
