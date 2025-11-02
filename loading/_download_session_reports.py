import os
import time
import requests
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.firefox.service import Service
from selenium.webdriver.firefox.options import Options
from selenium.common.exceptions import TimeoutException, NoSuchDriverException

options = Options()
options.headless = False

try:
    driver = webdriver.Firefox(service=Service(os.path.abspath("./geckodriver.exe")), options=options)
except NoSuchDriverException:
    raise NoSuchDriverException('To download lap charts, Gecko Driver must be installed in the working directory. '
                                'You can download Gecko Driver here: https://github.com/mozilla/geckodriver/releases')
    
def download_session_reports(firstYear,lastYear):
    for YEAR in range(firstYear, lastYear+1):
        driver.get("https://www.indycar.com/Results")
    
        wait = WebDriverWait(driver, 10)
    
        # Select the year from dropdown
        button = wait.until(EC.element_to_be_clickable((By.ID, "season-select-button-race")))
        button.click()
        
        # Step 2: Wait for the dropdown to become visible and clickable
        year_option = wait.until(EC.element_to_be_clickable(
            (By.XPATH, f"//div[@class='custom-select-menu show']//a[text()='{YEAR}']")))
        
        # Scroll the year option into view
        driver.execute_script("arguments[0].scrollIntoView(true);", year_option)
        time.sleep(0.5)  # let the scroll complete
        
        # Try normal click, fall back to JavaScript click if needed
        try:
            year_option.click()
        except Exception:
            driver.execute_script("arguments[0].click();", year_option)
    
    
        time.sleep(2)  # Let races populate
        
        # Select the year from dropdown
        button = wait.until(EC.element_to_be_clickable((By.ID, "race-select-button")))
        button.click()
    
        time.sleep(2)  # Let races populate
    
        # Get all race options
        race_options = wait.until(EC.presence_of_all_elements_located(
        (By.XPATH, "//div[contains(@class, 'custom-select-menu') and contains(@class, 'show')]//a")))
    
        for r in race_options:
            race_name = r.text.strip()
            r.click()
    
            time.sleep(2)
    
            for pdftype in ['eventsummary','leaderlapsummary','pitstopsummary','boxscore',
                            'lapchart','results','sectionresults','topsectiontimes']:
                try:
                    link = WebDriverWait(driver, 5).until(EC.presence_of_element_located((By.CSS_SELECTOR, f"a[href*='{pdftype}'][href$='.pdf']")))
                except TimeoutException:
                    print(f'No {pdftype} pdf for {YEAR} {race_name}')
                    continue
                
                pdf_url = link.get_attribute("href")
        
                if pdf_url:
                    race_id, date = pdf_url.split('/')[-3:-1]
                    filename = f"{pdftype}_{date}_{race_id}_{race_name}.pdf"
                    if filename not in os.listdir(f'./pdfs/{pdftype}'):
                        filepath = os.path.join("./pdfs",pdftype, filename)
                        r = requests.get(pdf_url)
                        with open(filepath, "wb") as f:
                            f.write(r.content)
    
    
            # click the race options button again
            button = wait.until(EC.element_to_be_clickable((By.ID, "race-select-button")))
            button.click()
               
    driver.quit()