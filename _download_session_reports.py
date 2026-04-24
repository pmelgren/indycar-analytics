import os
import time
import requests
from pathlib import Path
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.firefox.service import Service
from selenium.webdriver.firefox.options import Options
from selenium.common.exceptions import TimeoutException, NoSuchDriverException

options = Options()
options.headless = False

def wait_for_overlay_to_clear(driver, timeout=10):
    WebDriverWait(driver, timeout).until(
        EC.invisibility_of_element_located((By.CLASS_NAME, "loading-overlay"))
    )

    
def download_session_reports(firstYear,lastYear):
    
    firefox_binary_paths = [
        "C:\\Program Files\\Mozilla Firefox\\firefox.exe",
        "C:\\Program Files (x86)\\Mozilla Firefox\\firefox.exe",
    ]
    
    firefox_binary = None
    for path in firefox_binary_paths:
        if os.path.exists(path):
            firefox_binary = path
            break
    
    if firefox_binary:
        options.binary_location = firefox_binary
    
        driver = webdriver.Firefox(service=Service(os.path.abspath("./geckodriver.exe")), options=options)

    
    for YEAR in range(firstYear, lastYear+1):
        driver.get("https://www.indycar.com/Results")
        wait_for_overlay_to_clear(driver)

        wait = WebDriverWait(driver, 10)
    
        button = wait.until(EC.element_to_be_clickable((By.ID, "season-select-button-race")))
        driver.execute_script("arguments[0].click();", button)

        
        year_option = wait.until(EC.element_to_be_clickable(
            (By.XPATH, f"//div[@class='custom-select-menu show']//a[text()='{YEAR}']")))
        
        driver.execute_script("arguments[0].scrollIntoView(true);", year_option)
        time.sleep(0.5)
        wait_for_overlay_to_clear(driver)
        
        try:
            year_option.click()
            wait_for_overlay_to_clear(driver)
        except Exception:
            driver.execute_script("arguments[0].click();", year_option)
    
        print(YEAR)
        time.sleep(2)
        
        button = wait.until(EC.element_to_be_clickable((By.ID, "race-select-button")))
        button.click()
        wait_for_overlay_to_clear(driver)
    
        time.sleep(2)
    
        race_options = wait.until(EC.presence_of_all_elements_located(
            (By.XPATH, "//div[contains(@class, 'custom-select-menu') and contains(@class, 'show')]//a")))

        race_names = []
        for r in race_options:
            race_names.append(r.text.strip())
        
        for race_name in race_names:
            print(f"\n{race_name}")
            try:
                if "'" in race_name:
                    xpath = f'//div[contains(@class, "custom-select-menu") and contains(@class, "show")]//a[contains(text(), "{race_name}")]'
                else:
                    xpath = f"//div[contains(@class, 'custom-select-menu') and contains(@class, 'show')]//a[contains(text(), '{race_name}')]"
                
                race_option = wait.until(EC.element_to_be_clickable((By.XPATH, xpath)))
                race_option.click()           
        
                time.sleep(2)
        
                session_tabs = driver.find_elements(By.CSS_SELECTOR, "div.race-tabs button.tab")
                
                session_names = [tab.text.strip() for tab in session_tabs]
                
                for i, session_name in enumerate(session_names):
                    print(f"  Session: {session_name}")
                    session_tabs[i].click()
                    time.sleep(2)
                    
                    date_elem = driver.find_element(By.CSS_SELECTOR, "p.tabs-details-descriptor")
                    session_date_text = date_elem.text.strip()
                    session_date = datetime.strptime(session_date_text, "%A, %B %d, %Y").strftime("%Y%m%d")
                    
                    reports_section = driver.find_element(By.ID, "reports-content")
                    pdf_links = reports_section.find_elements(By.CSS_SELECTOR, "a[href$='.pdf']")
                    
                    for link in pdf_links:
                        pdf_url = link.get_attribute("href")
                        report_name = link.get_attribute("id").replace('-btn','').replace('-','')
                        if not pdf_url:
                            continue
                        
                        url_parts = pdf_url.split('/')
                        race_id = url_parts[-3]
                        
                        safe_race_name = race_name.replace("'", "").replace(" ", "_")
                        safe_session_name = session_name.replace("'", "").replace(" ", "_")
                        filename = f"{session_date}_{race_id}_{safe_race_name}_{safe_session_name}_{report_name}.pdf"
                        filepath = os.path.join("./pdfs", report_name, filename)
                        
                        if os.path.exists(filepath):
                            print(f"    Skipping {report_name} (already exists)")
                            continue
                        
                        print(f"    Downloading {report_name}")
                        os.makedirs(os.path.dirname(filepath), exist_ok=True)
                        r = requests.get(pdf_url)
                        if r.status_code == 404:
                            print(f"    404 error for {report_name}, skipping")
                            continue
                        r.raise_for_status()
                        with open(filepath, "wb") as f:
                            f.write(r.content)
            except Exception as e:
                print(f"  Error processing race: {e}")
            finally:
                button = wait.until(EC.element_to_be_clickable((By.ID, "race-select-button")))
                driver.execute_script("arguments[0].scrollIntoView(true);", button)
                time.sleep(0.5)
                driver.execute_script("arguments[0].click();", button)
               
    driver.quit()

if __name__ == "__main__":
    download_session_reports(2012, 2026)
