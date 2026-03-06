from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
import time


BASE_URL = "https://preply.com/en/online/italian-tutors"


def get_all_subject_links():

    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()))
    wait = WebDriverWait(driver, 20)

    driver.get(BASE_URL)

    urls = set()

    with open("subject_urls.txt", "w", encoding="utf-8") as f:

        index = 0

        while True:

            driver.get(BASE_URL)

            input_box = wait.until(
                EC.element_to_be_clickable((By.ID, "AutocompleteSelectInput"))
            )
            input_box.click()

            time.sleep(2)

            items = wait.until(
                EC.presence_of_all_elements_located(
                    (By.XPATH, "//div[contains(@data-qa-id,'subj-filter-')]")
                )
            )

            if index >= len(items):
                break

            subject_name = items[index].text.strip()

            print("Clicking:", subject_name)

            items[index].click()

            wait.until(lambda d: "tutors" in d.current_url)

            url = driver.current_url

            if url not in urls:
                urls.add(url)
                f.write(url + "\n")
                f.flush()
                print("✓ Saved:", url)

            index += 1

    driver.quit()

    print("\nTotal subjects:", len(urls))


if __name__ == "__main__":
    get_all_subject_links()