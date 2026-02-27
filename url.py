from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
import time
import re


def slugify(text):
    text = text.lower()
    text = re.sub(r"[^\w\s-]", "", text)
    text = text.replace(" ", "-")
    return text


def get_all_subject_links():

    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()))
    wait = WebDriverWait(driver, 15)

    driver.get("https://preply.com/en/online/italian-tutors")

    input_box = wait.until(
        EC.element_to_be_clickable((By.ID, "AutocompleteSelectInput"))
    )
    input_box.click()
    time.sleep(2)

    # Get all suggestion items
    items = driver.find_elements(By.XPATH, "//div[contains(@data-qa-id,'subj-filter-')]")

    urls = set()

    with open("subject_urls.txt", "w", encoding="utf-8") as f:
        for item in items:
            subject_name = item.text.strip()

            if subject_name:
                slug = slugify(subject_name)
                url = f"https://preply.com/en/online/{slug}-tutors"
                urls.add(url)

                f.write(url + "\n")
                f.flush()
                print("✓ Saved:", url)

    driver.quit()
    print("\nTotal subjects:", len(urls))


if __name__ == "__main__":
    get_all_subject_links()