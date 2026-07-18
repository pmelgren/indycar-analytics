import os
import time
import requests
from pathlib import Path
from datetime import datetime
from urllib.parse import urlparse
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.firefox.service import Service
from selenium.webdriver.firefox.options import Options
from selenium.common.exceptions import (
    TimeoutException,
    NoSuchDriverException,
    ElementClickInterceptedException,
    StaleElementReferenceException,
)

options = Options()
options.headless = False


def normalize_race_name_token(race_name):
    normalized = race_name.replace("'", "").replace(";", "_").replace("_", " ").strip()
    return "_".join(normalized.title().split())


def normalize_session_name_token(session_name):
    normalized = session_name.replace("'", "").replace(";", "_").replace(" ", "_")
    return normalized.upper()


def save_results_table_html(driver, session_date, race_name, session_name, series_tag=""):
    table_html = driver.find_element(By.ID, "race-results-table").get_attribute("outerHTML")
    safe_race_name = normalize_race_name_token(race_name)
    safe_session_name = normalize_session_name_token(session_name)
    if series_tag:
        filename = f"{session_date};{safe_race_name};{safe_session_name};results;{series_tag}.html"
        filepath = os.path.join("./data", "html", "results", filename)
    else:
        filename = f"{session_date};{safe_race_name};{safe_session_name};results.html"
        filepath = os.path.join("./data", "html", "results", filename)
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    with open(filepath, "w", encoding="utf-8") as f:
        f.write(table_html)
    print("    Saved html results table")

def wait_for_overlay_to_clear(driver, timeout=10):
    WebDriverWait(driver, timeout).until(
        EC.invisibility_of_element_located((By.CLASS_NAME, "loading-overlay"))
    )


def click_with_retry(driver, locator, timeout=10, attempts=3):
    last_error = None
    for _ in range(attempts):
        try:
            wait_for_overlay_to_clear(driver, timeout)
            elem = WebDriverWait(driver, timeout).until(EC.presence_of_element_located(locator))
            driver.execute_script("arguments[0].scrollIntoView({block: 'center', inline: 'nearest'});", elem)
            WebDriverWait(driver, timeout).until(EC.element_to_be_clickable(locator))
            try:
                elem.click()
            except ElementClickInterceptedException:
                wait_for_overlay_to_clear(driver, timeout)
                driver.execute_script("arguments[0].click();", elem)
            wait_for_overlay_to_clear(driver, timeout)
            return
        except (TimeoutException, ElementClickInterceptedException, StaleElementReferenceException) as e:
            last_error = e
            time.sleep(0.5)
    if last_error:
        raise last_error


def recover_from_object_moved_page(driver):
    body_text = driver.find_element(By.TAG_NAME, "body").text
    if "Object moved to" not in body_text:
        return False

    redirect_links = driver.find_elements(By.XPATH, "//a[normalize-space(text())='here']")
    for link in redirect_links:
        href = link.get_attribute("href")
        if href and "/results/" in href.lower():
            print("  Redirect page detected, following results link")
            driver.get(href)
            wait_for_overlay_to_clear(driver)
            time.sleep(2)
            return True

    return False


def process_current_race(driver, wait, race_name, series_tag=""):
    wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "div.race-tabs button.tab")))
    wait_for_overlay_to_clear(driver)
    time.sleep(2)

    session_tabs = driver.find_elements(By.CSS_SELECTOR, "div.race-tabs button.tab")
    session_names = [tab.text.strip() for tab in session_tabs]

    for i, session_name in enumerate(session_names):
        try:
            print(f"  Session: {session_name}")
            # some years the race session is referred to by the event name, not by "Race"
            if (race_name.lower() in session_name.lower()) or (session_name.lower() in race_name.lower()):
                session_name = 'RACE'
                
            session_tab_locator = (By.CSS_SELECTOR, f"div.race-tabs button.tab:nth-of-type({i + 1})")
            session_tab = wait.until(EC.presence_of_element_located(session_tab_locator))
            if "active" not in session_tab.get_attribute("class"):
                click_with_retry(driver, session_tab_locator)
            else:
                wait_for_overlay_to_clear(driver)
            time.sleep(2)

            date_elem = driver.find_element(By.CSS_SELECTOR, "p.tabs-details-descriptor")
            session_date_text = date_elem.text.strip()
            session_date = datetime.strptime(session_date_text, "%A, %B %d, %Y").strftime("%Y%m%d")
            save_results_table_html(driver, session_date, race_name, session_name, series_tag)

            reports_section = driver.find_element(By.ID, "reports-content")
            pdf_links = reports_section.find_elements(By.CSS_SELECTOR, "a[href$='.pdf']")

            for link in pdf_links:
                try:
                    pdf_url = link.get_attribute("href")
                    report_name = link.get_attribute("id").replace('-btn','').replace('-','')
                    if not pdf_url:
                        continue

                    url_parts = pdf_url.split('/')
                    race_id = url_parts[-3]

                    safe_race_name = normalize_race_name_token(race_name)
                    safe_session_name = normalize_session_name_token(session_name)
                    safe_report_name = report_name.replace(";", "_")
                    if series_tag:
                        filename = f"{session_date};{race_id};{safe_race_name};{safe_session_name};{safe_report_name};{series_tag}.pdf"
                        filepath = os.path.join("./data", "pdfs", report_name, filename)
                    else:
                        filename = f"{session_date};{race_id};{safe_race_name};{safe_session_name};{safe_report_name}.pdf"
                        filepath = os.path.join("./data", "pdfs", report_name, filename)

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
                    print(f"    Error processing report {report_name if 'report_name' in locals() else '(unknown)'}: {e}")
                    continue
        except Exception as e:
            print(f"  Error processing session {session_name}: {e}")
            continue

    
def download_session_reports(firstYear=None, lastYear=None, race_url=None, site_domain="indycar.com"):
    
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

    wait = WebDriverWait(driver, 10)

    if race_url:
        race_url_domain = urlparse(race_url).netloc.lower()
        if "indynxt.com" in race_url_domain:
            site_domain = "indynxt.com"
        elif "indycar.com" in race_url_domain:
            site_domain = "indycar.com"

    series_tag = "indynxt" if "indynxt" in site_domain.lower() else ""
    results_url = f"https://www.{site_domain}/results"

    if race_url:
        driver.get(race_url)
        wait_for_overlay_to_clear(driver)
        try:
            race_name = wait.until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "p.tabs-details-header"))
            ).text.strip()
        except TimeoutException:
            race_name = "single_race"

        print(f"\n{race_name}")
        process_current_race(driver, wait, race_name, series_tag)
        driver.quit()
        return

    if firstYear is None or lastYear is None:
        raise ValueError("firstYear and lastYear are required when race_url is not provided")

    
    for YEAR in range(firstYear, lastYear+1):
        driver.get(results_url)
        wait_for_overlay_to_clear(driver)
    
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

        if not driver.find_elements(By.ID, "race-select-button"):
            recover_from_object_moved_page(driver)
        
        button = wait.until(EC.element_to_be_clickable((By.ID, "race-select-button")))
        button.click()
        wait_for_overlay_to_clear(driver)
    
        time.sleep(2)
    
        race_options = wait.until(EC.presence_of_all_elements_located(
            (By.XPATH, "//div[contains(@class, 'custom-select-menu') and contains(@class, 'show')]//a")))

        race_names = []
        for r in race_options:
            race_names.append(r.text.strip())

        print(race_names)

        button = wait.until(EC.element_to_be_clickable((By.ID, "race-select-button")))
        driver.execute_script("arguments[0].scrollIntoView(true);", button)
        time.sleep(0.5)
        driver.execute_script("arguments[0].click();", button)
        wait_for_overlay_to_clear(driver)
        
        for race_name in race_names:
            print(f"\n{race_name}")
            race_processed = False
            for attempt in range(2):
                try:
                    button = wait.until(EC.element_to_be_clickable((By.ID, "race-select-button")))
                    driver.execute_script("arguments[0].scrollIntoView(true);", button)
                    time.sleep(0.5)
                    driver.execute_script("arguments[0].click();", button)
                    wait_for_overlay_to_clear(driver)

                    if "'" in race_name:
                        xpath = f'//div[contains(@class, "custom-select-menu") and contains(@class, "show")]//a[contains(text(), "{race_name}")]'
                    else:
                        xpath = f"//div[contains(@class, 'custom-select-menu') and contains(@class, 'show')]//a[contains(text(), '{race_name}')]"

                    click_with_retry(driver, (By.XPATH, xpath))
                    process_current_race(driver, wait, race_name, series_tag)
                    race_processed = True
                    break
                except Exception as e:
                    if attempt == 0:
                        driver.get(results_url)
                        wait_for_overlay_to_clear(driver)

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

                        if not driver.find_elements(By.ID, "race-select-button"):
                            recover_from_object_moved_page(driver)
                    else:
                        print(f"  Error processing race: {e}")
            if not race_processed:
                continue
               
    driver.quit()
