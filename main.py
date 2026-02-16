import os
import requests
import psycopg2
from jobspy import scrape_jobs

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
    requests.post(tg_url, json=payload)

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

if __name__ == "__main__":
    init_db()
    run_pipeline()
