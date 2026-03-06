import os
import requests
from bs4 import BeautifulSoup
import csv
import time
import random
import math
import re
import logging

# Log file so you can check on the server that the job ran
LOG_FILE = os.environ.get("PREPLY_SCRAPER_LOG", "preply_scraper.log")


def setup_logging():
    """Write to preply_scraper.log (with timestamps) and also to console."""
    global logger
    fmt = "%(asctime)s | %(levelname)s | %(message)s"
    date_fmt = "%Y-%m-%d %H:%M:%S"
    logging.basicConfig(
        level=logging.INFO,
        format=fmt,
        datefmt=date_fmt,
        handlers=[
            logging.FileHandler(LOG_FILE, encoding="utf-8"),
            logging.StreamHandler(),
        ],
    )
    logger = logging.getLogger(__name__)
    return logger


logger = None  # set by setup_logging() when run as __main__

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept-Language': 'en-US,en;q=0.9',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
    'Accept-Encoding': 'gzip, deflate, br',
    'Connection': 'keep-alive',
    'Referer': 'https://preply.com/',
}


def safe_text(tag):
    return tag.get_text(strip=True) if tag else 'N/A'


def extract_tutor_details(tutor_div):
    try:

        # ── TUTOR ID & PROFILE URL ────────────────────────────────────────────
        # <section data-qa-tutor-id="6685091">
        tutor_id    = tutor_div.get('data-qa-tutor-id', 'N/A')
        profile_url = f'https://preply.com/en/tutor/{tutor_id}' if tutor_id != 'N/A' else 'N/A'

        # ── NAME ──────────────────────────────────────────────────────────────
        # <a class="styles_FullName__scSqc"> → <h4>Maayan D.</h4>
        name = 'N/A'
        name_anchor = tutor_div.find('a', class_=lambda c: c and 'FullName' in c)
        if name_anchor:
            h4 = name_anchor.find('h4')
            name = safe_text(h4)

        # ── COUNTRY ───────────────────────────────────────────────────────────
        # <img class="flag__wPkCc" alt="Israel">
        country = 'N/A'
        flag_img = tutor_div.find('img', class_=lambda c: c and 'flag' in c)
        if flag_img:
            country = flag_img.get('alt', 'N/A')

        # ── PROFILE BADGE (Professional / Super Tutor etc.) ───────────────────
        # captures both Professional and Super Tutor badges

        badge = 'N/A'

        badges = []

        badge_labels = tutor_div.select('span.badge__Aj95L span.label__F86ML')

        for b in badge_labels:
            text = safe_text(b)
            if text not in badges:
                badges.append(text)

        if badges:
            badge = ", ".join(badges)
        # ── PROFILE IMAGE ─────────────────────────────────────────────────────
        # <img alt="Tutor english Maayan D." src="...">
        image_url = 'N/A'
        tutor_img = tutor_div.find('img', alt=lambda a: a and 'Tutor' in a)
        if tutor_img:
            image_url = tutor_img.get('src', 'N/A')

        # ── ONLINE STATUS ─────────────────────────────────────────────────────
        # <div class="styles_newOnlineBadge__WXKjK styles_onlineBadgeOffline__jXJii ...">
        online_status = 'Offline'
        online_badge = tutor_div.find('div', class_=lambda c: c and 'newOnlineBadge' in c)
        if online_badge:
            classes = ' '.join(online_badge.get('class', []))
            online_status = 'Offline' if 'Offline' in classes or 'offline' in classes else 'Online'

        # ── PRICE ─────────────────────────────────────────────────────────────
        # <h4 data-qa-group="tutor-price-value"><span>$40</span></h4>
        price = 'N/A'
        price_tag = tutor_div.find(attrs={'data-qa-group': 'tutor-price-value'})
        if price_tag:
            price = price_tag.get_text(strip=True)

        # ── LESSON DURATION ───────────────────────────────────────────────────
        # <p data-preply-ds-component="Text">50-min lesson</p>
        lesson_duration = 'N/A'
        for p in tutor_div.find_all('p', {'data-preply-ds-component': 'Text'}):
            txt = p.get_text(strip=True)
            if 'min lesson' in txt.lower():
                lesson_duration = txt
                break

        # ── RATING ────────────────────────────────────────────────────────────
        # Desktop: <button class="styles_reviewsButton__SfuGT"> → <h5>5</h5>
        # Mobile:  <button class="styles_RatingIndicator__Hg6b4"> → <h4>5</h4>
        rating  = 'N/A'
        reviews = 'N/A'

        review_btn = tutor_div.find('button', class_=lambda c: c and 'reviewsButton' in c)
        if review_btn:
            h5 = review_btn.find('h5')
            rating = safe_text(h5)
            rev_p = review_btn.find('p', {'data-preply-ds-component': 'Text'})
            reviews = safe_text(rev_p).replace('reviews', '').replace('review', '').strip()
        else:
            rating_btn = tutor_div.find('button', class_=lambda c: c and 'RatingIndicator' in c)
            if rating_btn:
                h4 = rating_btn.find('h4')
                rating = safe_text(h4)
                span = rating_btn.find('span', {'data-preply-ds-component': 'Text'})
                reviews = safe_text(span).replace('reviews', '').replace('review', '').strip()

        # ── STUDENTS ──────────────────────────────────────────────────────────
        # <div class="Text__uVacy ..."><p ...>10</p> students</div>
        students = 'N/A'
        for div in tutor_div.find_all('div', {'data-preply-ds-component': 'Text'}):
            if 'student' in div.get_text().lower():
                p = div.find('p')
                if p:
                    students = safe_text(p)
                    break

        # ── LESSONS ───────────────────────────────────────────────────────────
        # <div class="Text__uVacy ..."><p ...>218</p> lessons</div>
        lessons = 'N/A'
        for div in tutor_div.find_all('div', {'data-preply-ds-component': 'Text'}):
            full = div.get_text()
            if 'lesson' in full.lower() and 'min lesson' not in full.lower():
                p = div.find('p')
                if p:
                    lessons = safe_text(p)
                    break

        # ── TEACHES (subject) ─────────────────────────────────────────────────
        # <ul data-preply-ds-component="LayoutFlex"><li>English</li></ul>
        teaches = 'N/A'
        main_info = tutor_div.find('div', class_=lambda c: c and 'MainInfoWrapper' in c)
        if main_info:
            teaches_ul = main_info.find('ul', {'data-preply-ds-component': 'LayoutFlex'})
            if teaches_ul:
                items = teaches_ul.find_all('li')
                teaches = ', '.join([safe_text(li) for li in items])

        # ── SPEAKS / LANGUAGES ────────────────────────────────────────────────
        # <ul class="styles_SpeaksList__Rlshm"><li>English (Proficient)</li><li>Hebrew (Native)</li></ul>
        speaks = 'N/A'
        speaks_ul = tutor_div.find('ul', class_=lambda c: c and 'SpeaksList' in c)
        if speaks_ul:
            items = speaks_ul.find_all('li')
            speaks = ', '.join([safe_text(li) for li in items])
            # Check for hidden languages e.g. "+2"
            more_langs = tutor_div.find('span', class_=lambda c: c and 'ShowRestLanguages' in c)
            if more_langs:
                speaks += f' {safe_text(more_langs)} more'

        # ── DESCRIPTION ───────────────────────────────────────────────────────
        # <p class="styles_SeoSnippetContent__JPwTq">
        #   <span>Title bold part</span> — <span>body part</span>
        # </p>
        desc_title = 'N/A'
        desc_body  = 'N/A'
        desc_p = tutor_div.find('p', class_=lambda c: c and 'SeoSnippetContent' in c)
        if desc_p:
            spans = desc_p.find_all('span', {'data-preply-ds-component': 'Text'})
            if len(spans) >= 1:
                desc_title = safe_text(spans[0])
            if len(spans) >= 2:
                desc_body = safe_text(spans[1])

        print(f"  ✓ {name} | {country} | {badge} | {price} | {lesson_duration} | "
              f"⭐{rating} ({reviews} reviews) | 👥{students} students | "
              f"📚{lessons} lessons | {speaks}")

        return {
            'tutor_id':        tutor_id,
            'name':            name,
            'profile_url':     profile_url,
            'country':         country,
            'badge':           badge,
            'image_url':       image_url,
            'online_status':   online_status,
            'price':           price,
            'lesson_duration': lesson_duration,
            'rating':          rating,
            'reviews':         reviews,
            'students':        students,
            'lessons':         lessons,
            'teaches':         teaches,
            'speaks':          speaks,
            'desc_title':      desc_title,
            'desc_body':       desc_body,
        }

    except Exception as e:
        print(f"  ✗ Error: {e}")
        return None


# Retry config for non-200 responses
MAX_RETRIES = 3
RETRY_DELAY_BASE = 10  # seconds
RETRY_DELAY_429 = 60   # seconds for rate limit


def scrape_page(page_url, writer, session):
    """Fetch a page with retries. Returns (tutor_count, success).
    success=False means we never got 200 (caller should try next page, not break).
    """
    last_exception = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = session.get(page_url, headers=HEADERS, timeout=20)

            if response.status_code == 429:
                wait = RETRY_DELAY_429
                print(f"  Rate limited. Waiting {wait}s (attempt {attempt}/{MAX_RETRIES})...")
                time.sleep(wait)
                response = session.get(page_url, headers=HEADERS, timeout=20)

            if response.status_code != 200:
                print(f"  Failed — HTTP {response.status_code} (attempt {attempt}/{MAX_RETRIES})")
                if attempt < MAX_RETRIES:
                    delay = RETRY_DELAY_BASE * attempt
                    print(f"  Retrying in {delay}s...")
                    time.sleep(delay)
                else:
                    print("  All retries exhausted — skipping to next page.")
                    return 0, False
                continue

            # Got 200 — parse and return
            soup = BeautifulSoup(response.content, 'html.parser')
            tutor_divs = soup.find_all('section', {'data-qa-group': 'tutor-profile'})

            if not tutor_divs:
                print("  No tutor cards found — possibly blocked.")
                with open('debug_page.html', 'w', encoding='utf-8') as f:
                    f.write(response.text)
                print("  Raw HTML saved to debug_page.html")
                return 0, True  # success=True so caller can apply "batch < 10" rule

            print(f"  Found {len(tutor_divs)} tutors.")
            count = 0
            for tutor_div in tutor_divs:
                tutor = extract_tutor_details(tutor_div)
                if tutor:
                    writer.writerow(tutor)
                    count += 1
            return count, True

        except requests.exceptions.Timeout:
            last_exception = "Timeout"
            print(f"  Timeout (attempt {attempt}/{MAX_RETRIES}).")
            if attempt < MAX_RETRIES:
                delay = RETRY_DELAY_BASE * attempt
                print(f"  Retrying in {delay}s...")
                time.sleep(delay)
            else:
                print("  All retries exhausted — skipping to next page.")
                return 0, False
        except Exception as e:
            last_exception = e
            print(f"  Page error: {e} (attempt {attempt}/{MAX_RETRIES})")
            if attempt < MAX_RETRIES:
                delay = RETRY_DELAY_BASE * attempt
                print(f"  Retrying in {delay}s...")
                time.sleep(delay)
            else:
                print("  All retries exhausted — skipping to next page.")
                return 0, False

    return 0, False


def scrape_all_pages(base_url, total_pages, start_page=1, output_file='preply_tutors_luganda.csv'):
    total_tutors = 0
    session = requests.Session()
    write_mode = 'w' if start_page == 1 else 'a'

    with open(output_file, mode=write_mode, newline='', encoding='utf-8') as file:
        fieldnames = [
            'tutor_id', 'name', 'profile_url', 'country', 'badge',
            'image_url', 'online_status', 'price', 'lesson_duration',
            'rating', 'reviews', 'students', 'lessons', 'teaches',
            'speaks', 'desc_title', 'desc_body'
        ]
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        if write_mode == 'w':
            writer.writeheader()

        for page in range(start_page, total_pages + 1):
            print(f"\nScraping page {page}/{total_pages}...")
            count, success = scrape_page(f"{base_url}?page={page}", writer, session)
            total_tutors += count
            print(f"  → Saved: {count} | Total so far: {total_tutors}")
            file.flush()
            # Only break when we successfully fetched and got fewer than 10 tutors (last page)
            if success and count < 10:
                print(f"  Last page reached (batch {count} < 10). Stopping.")
                break
            time.sleep(random.uniform(2.0, 4.5))

    print(f"\n✅ Done! {total_tutors} tutors saved to {output_file}")
    if logger:
        logger.info("Subject saved %d tutors to %s", total_tutors, output_file)

import re
import math

def get_total_pages(session, base_url):

    response = session.get(base_url, headers=HEADERS, timeout=20)
    soup = BeautifulSoup(response.content, "html.parser")

    # ─────────────────────────────────────────
    # METHOD 1 — DIRECT PAGE COUNT (BEST)
    # ─────────────────────────────────────────
    page_span = soup.find(
        "span",
        attrs={"data-preply-ds-component": "Text"},
        string=lambda s: s and s.strip().isdigit()
    )

    if page_span:
        pages = int(page_span.text.strip())
        print(f"Detected pages directly: {pages}")
        return pages

    # ─────────────────────────────────────────
    # METHOD 2 — TOTAL TUTOR COUNT (FALLBACK)
    # ─────────────────────────────────────────
    spans = soup.find_all("span", class_=lambda c: c and "ButtonBase--content" in c)

    for span in spans:
        text = span.get_text(strip=True)

        if "Show" in text and "tutors" in text:
            match = re.search(r"([\d,]+)", text)
            if match:
                total_tutors = int(match.group(1).replace(",", ""))
                pages = math.ceil(total_tutors / 10)

                print(f"Total tutors: {total_tutors}")
                print(f"Calculated pages: {pages}")

                return pages

    # ─────────────────────────────────────────
    # LAST RESORT
    # ─────────────────────────────────────────
    print("Could not detect page count. Defaulting to 1 page.")
    return 1

def extract_subject_from_url(url):
    """
    Extract subject name from:
    https://preply.com/en/online/luganda-tutors
    """
    slug = url.rstrip("/").split("/")[-1]
    slug = slug.replace("-tutors", "")
    return slug

def scrape_until_end(base_url, output_file):

    session = requests.Session()
    total_tutors = 0
    page = 1
    tutors_per_page = None

    with open(output_file, "w", newline="", encoding="utf-8") as file:

        fieldnames = [
            'tutor_id','name','profile_url','country','badge',
            'image_url','online_status','price','lesson_duration',
            'rating','reviews','students','lessons','teaches',
            'speaks','desc_title','desc_body'
        ]

        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()

        while True:

            page_url = f"{base_url}?page={page}"

            print(f"\nScraping page {page}...")

            response = None
            for attempt in range(1, MAX_RETRIES + 1):
                try:
                    response = session.get(page_url, headers=HEADERS, timeout=20)
                    if response.status_code == 429:
                        print(f"  Rate limited. Waiting {RETRY_DELAY_429}s (attempt {attempt}/{MAX_RETRIES})...")
                        time.sleep(RETRY_DELAY_429)
                        response = session.get(page_url, headers=HEADERS, timeout=20)
                    if response.status_code != 200:
                        print(f"  HTTP {response.status_code} (attempt {attempt}/{MAX_RETRIES})")
                        if attempt < MAX_RETRIES:
                            time.sleep(RETRY_DELAY_BASE * attempt)
                            continue
                        print("  All retries exhausted — skipping to next page.")
                        page += 1
                        response = None
                        break
                    break  # got 200
                except (requests.exceptions.Timeout, Exception) as e:
                    print(f"  Error: {e} (attempt {attempt}/{MAX_RETRIES})")
                    if attempt < MAX_RETRIES:
                        time.sleep(RETRY_DELAY_BASE * attempt)
                    else:
                        print("  All retries exhausted — skipping to next page.")
                        page += 1
                        response = None
                        break

            if response is None or response.status_code != 200:
                continue

            soup = BeautifulSoup(response.content, "html.parser")

            tutor_divs = soup.find_all('section', {'data-qa-group': 'tutor-profile'})

            if not tutor_divs:
                print("No tutors found — stopping.")
                break

            if tutors_per_page is None:
                tutors_per_page = len(tutor_divs)
                print(f"Detected tutors per page: {tutors_per_page}")

            count = 0

            for tutor_div in tutor_divs:
                tutor = extract_tutor_details(tutor_div)
                if tutor:
                    writer.writerow(tutor)
                    count += 1
                    total_tutors += 1

            print(f"Saved {count} tutors")

            # Stop only when we got a successful fetch and batch is less than 10
            if count < 10:
                print("Last page reached (batch < 10).")
                break

            page += 1
            time.sleep(random.uniform(2.0,4.5))

    print(f"\nScraped total tutors: {total_tutors}")
    if logger:
        logger.info("Subject (dynamic) saved %d tutors", total_tutors)

def log_404(url):
    with open("invalid_subject_urls.txt", "a", encoding="utf-8") as f:
        f.write(url + "\n")
        f.flush()
    print("Logged 404:", url)

if __name__ == "__main__":

    logger = setup_logging()
    logger.info("Scraper run started")

    session = requests.Session()

    with open("subject_urls.txt", "r", encoding="utf-8") as f:
        subject_urls = [line.strip() for line in f if line.strip()]

    logger.info("Subjects to process: %d | Log file: %s", len(subject_urls), LOG_FILE)
    subjects_done = 0

    try:
        for base_url in subject_urls:

            print(f"\n===== Checking Subject URL: {base_url} =====")

            # Check if URL is valid before scraping
            response = session.get(base_url, headers=HEADERS, timeout=20)

            if response.status_code == 404:
                print("❌ 404 detected")
                log_404(base_url)
                logger.warning("404 skipped: %s", base_url)
                continue

            if response.status_code != 200:
                print(f"❌ Failed with HTTP {response.status_code}")
                log_404(base_url)
                logger.warning("HTTP %s skipped: %s", response.status_code, base_url)
                continue

            subject_name = extract_subject_from_url(base_url)
            logger.info("Subject started: %s", subject_name)

            print(f"===== Scraping Subject: {subject_name} =====")

            total_pages = get_total_pages(session, base_url)

            output_file = f"preply_tutors_data/preply_tutors_{subject_name}.csv"

            if total_pages > 1:
                scrape_all_pages(
                    base_url=base_url,
                    total_pages=total_pages,
                    start_page=1,
                    output_file=output_file
                )
            else:
                print("Page detection failed — switching to dynamic pagination")
                scrape_until_end(base_url, output_file)

            subjects_done += 1
            logger.info("Subject completed: %s -> %s", subject_name, output_file)

        logger.info("Scraper run finished successfully | Subjects completed: %d", subjects_done)
        logger.info("Scraper run ended")
        print(f"\n========== Scraper run complete | Subjects completed: {subjects_done} ==========")
        print(f"Scraper run ended. Check {LOG_FILE} for details.")

    except Exception as e:
        logger.exception("Scraper run failed: %s", e)
        print(f"\n========== Scraper run stopped with error ==========")
        raise