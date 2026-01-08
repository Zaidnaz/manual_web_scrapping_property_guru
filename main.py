import uvicorn
import csv
import os
import time
import json
import traceback
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from bs4 import BeautifulSoup
import google.generativeai as genai
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

# --- CONFIGURATION ---
GOOGLE_API_KEY = ""
genai.configure(api_key=GOOGLE_API_KEY)
model = genai.GenerativeModel('gemini-2.5-flash')

# Keeping your requested filename
CSV_FILE = "property_data_full.csv"

# --- CSV COLUMNS ---
CSV_HEADERS = [
    "Timestamp", "URL", 
    "Agent Name", "Agent Phone", "Agency Name", "Agent License",
    "Title", "Price", "Address", "District",
    "Property Type", "Tenure", "Built Year", "Developer",
    "Bedrooms", "Bathrooms", "Size (sqft)", "Price per sqft",
    "Furnishing", "Floor Level", "Facilities", "Description Summary"
]

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

driver_keeper = None

# --- HELPERS ---

def init_csv():
    """Creates the CSV file if it doesn't exist."""
    try:
        if not os.path.exists(CSV_FILE):
            print(f">>> 📁 Creating CSV at: {os.path.abspath(CSV_FILE)}")
            with open(CSV_FILE, mode='w', newline='', encoding='utf-8') as file:
                writer = csv.writer(file)
                writer.writerow(CSV_HEADERS)
    except PermissionError:
        print("!"*60)
        print("CRITICAL: CANNOT CREATE FILE. IS IT OPEN IN EXCEL?")
        print("!"*60)

def save_to_csv(data, url):
    """Appends data to the CSV file safely."""
    try:
        init_csv()
        row_data = [
            time.strftime("%Y-%m-%d %H:%M:%S"),
            url,
            data.get("agent_name", "N/A"),
            data.get("agent_phone", "N/A"),
            data.get("agency_name", "N/A"),
            data.get("agent_license", "N/A"),
            data.get("title", "N/A"),
            data.get("price", "N/A"),
            data.get("address", "N/A"),
            data.get("district", "N/A"),
            data.get("property_type", "N/A"),
            data.get("tenure", "N/A"),
            data.get("built_year", "N/A"),
            data.get("developer", "N/A"),
            data.get("bedrooms", "N/A"),
            data.get("bathrooms", "N/A"),
            data.get("size_sqft", "N/A"),
            data.get("psf", "N/A"),
            data.get("furnishing", "N/A"),
            data.get("floor_level", "N/A"),
            data.get("facilities", "N/A"),
            data.get("description_summary", "N/A")
        ]
        with open(CSV_FILE, mode='a', newline='', encoding='utf-8') as file:
            writer = csv.writer(file)
            writer.writerow(row_data)
        print(f">>> ✅ Saved row for: {data.get('agent_name', 'Unknown')}")
        
    except PermissionError:
        print("!"*60)
        print(f"❌ ERROR: Could not save to {CSV_FILE}.")
        print("PLEASE CLOSE THE FILE IN EXCEL AND TRY AGAIN.")
        print("!"*60)
    except Exception as e:
        print(f">>> ❌ CSV Error: {e}")

def clean_html(raw_html):
    """
    AGGRESSIVE CLEANING: Reduces token usage by removing non-essential HTML.
    """
    if not raw_html: return ""
    soup = BeautifulSoup(raw_html, 'html.parser')
    
    # 1. Remove Junk Tags
    for tag in soup(["script", "style", "svg", "noscript", "iframe", "meta", "link", "input", "button", "img", "video", "source", "picture"]):
        tag.decompose()

    # 2. Remove Structure Bloat (Navbars, Footers, Ads, Sidebars)
    # These contain huge text that consumes quota but has no info
    for tag in soup.find_all(['header', 'footer', 'nav', 'aside', 'form']):
        tag.decompose()
        
    # 3. Get Text and LIMIT IT
    # We cap at 25,000 characters to be safe for Free Tier
    return soup.get_text(separator=' ', strip=True)[:25000]

def safe_generate_content(prompt):
    """
    Wraps the Gemini call with simple retry logic for Manual Scraping.
    """
    # Wait times: 5s, 30s
    wait_times = [5, 30] 
    for attempt in range(len(wait_times)):
        try:
            return model.generate_content(prompt)
        except Exception as e:
            if "429" in str(e) or "quota" in str(e).lower():
                wait = wait_times[attempt]
                print(f">>> ⚠️ Token Limit Hit. Waiting {wait}s...")
                time.sleep(wait)
                continue
            raise e
    raise Exception("Failed to get response from Gemini (Quota Exceeded).")

# --- API ---

@app.post("/launch-browser")
async def launch_browser():
    global driver_keeper
    if driver_keeper:
        try:
            driver_keeper.current_url # Check if alive
            return {"message": "Browser is already open."}
        except:
            driver_keeper = None

    options = uc.ChromeOptions()
    options.headless = False
    options.add_argument('--no-sandbox')
    
    # Save profile to keep you logged in
    profile_path = os.path.join(os.getcwd(), "chrome_profile")
    options.add_argument(f"--user-data-dir={profile_path}")

    try:
        driver_keeper = uc.Chrome(options=options)
        driver_keeper.get("https://www.google.com") 
        return {"message": "Browser launched!"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/scrape-current-page")
async def scrape_current():
    global driver_keeper
    if not driver_keeper:
        raise HTTPException(status_code=400, detail="Browser not open.")
    
    try:
        print(">>> ⚡ Manual Scrape Started...")
        
        # --- SAFE CLICKING LOGIC ---
        keywords = ["Show Number", "View", "Call", "Contact", "WhatsApp"]
        for key in keywords:
            try:
                elements = driver_keeper.find_elements(By.XPATH, f"//*[contains(text(), '{key}')]")
                for el in elements:
                    try:
                        if el.tag_name.lower() == 'a': continue # Skip links
                        driver_keeper.execute_script("arguments[0].click();", el)
                        time.sleep(0.1) 
                    except: pass
            except: pass
        
        time.sleep(1) # Wait for reveal

        # Capture Data
        current_url = driver_keeper.current_url
        cleaned_text = clean_html(driver_keeper.page_source)
        
        print(f">>> 🧠 Analyzing Text ({len(cleaned_text)} chars)...")

        prompt = f"""
        Extract real estate parameters from this text into JSON.
        If a value is missing, use "N/A".

        Required Keys:
        agent_name, agent_phone, agency_name, agent_license,
        title, price, address, district,
        property_type, tenure, built_year, developer,
        bedrooms, bathrooms, size_sqft, psf,
        furnishing, floor_level, facilities, description_summary

        Text:
        {cleaned_text}
        
        Return JSON only.
        """
        
        # Use Safe Generator
        response = safe_generate_content(prompt)
        
        json_str = response.text.replace("```json", "").replace("```", "").strip()
        data = json.loads(json_str)
        
        save_to_csv(data, current_url)
        
        return {"status": "success", "data": data}

    except Exception as e:
        print(f"Error: {e}")
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    # Print CSV location
    print("-" * 50)
    print(f"Using CSV file: {os.path.abspath(CSV_FILE)}")
    print("-" * 50)
    uvicorn.run(app, host="127.0.0.1", port=8000)