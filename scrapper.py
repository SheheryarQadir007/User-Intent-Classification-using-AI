import requests
from bs4 import BeautifulSoup
import csv
import time
import random
import math
import re

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
        # <span class="badge__Aj95L"><span class="label__F86ML">Professional</span></span>
        badge = 'N/A'
        badge_tag = tutor_div.find('span', class_=lambda c: c and 'label__' in c)
        if badge_tag:
            badge = safe_text(badge_tag)

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


def scrape_page(page_url, writer, session):
    try:
        response = session.get(page_url, headers=HEADERS, timeout=20)

        if response.status_code == 429:
            print("  Rate limited. Waiting 60s...")
            time.sleep(60)
            response = session.get(page_url, headers=HEADERS, timeout=20)

        if response.status_code != 200:
            print(f"  Failed — HTTP {response.status_code}")
            return 0

        soup = BeautifulSoup(response.content, 'html.parser')
        tutor_divs = soup.find_all('section', {'data-qa-group': 'tutor-profile'})

        if not tutor_divs:
            print("  No tutor cards found — possibly blocked.")
            with open('debug_page.html', 'w', encoding='utf-8') as f:
                f.write(response.text)
            print("  Raw HTML saved to debug_page.html")
            return 0

        print(f"  Found {len(tutor_divs)} tutors.")
        count = 0
        for tutor_div in tutor_divs:
            tutor = extract_tutor_details(tutor_div)
            if tutor:
                writer.writerow(tutor)
                count += 1
        return count

    except requests.exceptions.Timeout:
        print("  Timeout. Skipping.")
        return 0
    except Exception as e:
        print(f"  Page error: {e}")
        return 0


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
            count = scrape_page(f"{base_url}?page={page}", writer, session)
            total_tutors += count
            print(f"  → Saved: {count} | Total so far: {total_tutors}")
            file.flush()
            time.sleep(random.uniform(2.0, 4.5))

    print(f"\n✅ Done! {total_tutors} tutors saved to {output_file}")

import re
import math

def get_total_pages(session, base_url):

    response = session.get(base_url, headers=HEADERS, timeout=20)
    soup = BeautifulSoup(response.content, "html.parser")

    total_tutors = None

    # Find ALL spans with that class
    spans = soup.find_all("span", class_=lambda c: c and "ButtonBase--content" in c)

    for span in spans:
        text = span.get_text(strip=True)

        if "Show" in text and "tutors" in text:
            match = re.search(r"([\d,]+)", text)
            if match:
                total_tutors = int(match.group(1).replace(",", ""))
                break

    if not total_tutors:
        print("Could not detect tutor count. Defaulting to 1 page.")
        return 100

    total_pages = math.ceil(total_tutors / 10)

    print(f"Total tutors: {total_tutors}")
    print(f"Total pages: {total_pages}")

    return total_pages

def extract_subject_from_url(url):
    """
    Extract subject name from:
    https://preply.com/en/online/luganda-tutors
    """
    slug = url.rstrip("/").split("/")[-1]
    slug = slug.replace("-tutors", "")
    return slug

def log_404(url):
    with open("invalid_subject_urls.txt", "a", encoding="utf-8") as f:
        f.write(url + "\n")
        f.flush()
    print("Logged 404:", url)

if __name__ == "__main__":

    session = requests.Session()

    with open("subject_urls.txt", "r", encoding="utf-8") as f:
        subject_urls = [line.strip() for line in f if line.strip()]

    for base_url in subject_urls:

        print(f"\n===== Checking Subject URL: {base_url} =====")

        # Check if URL is valid before scraping
        response = session.get(base_url, headers=HEADERS, timeout=20)

        if response.status_code == 404:
            print("❌ 404 detected")
            log_404(base_url)
            continue

        if response.status_code != 200:
            print(f"❌ Failed with HTTP {response.status_code}")
            log_404(base_url)
            continue

        subject_name = extract_subject_from_url(base_url)

        print(f"===== Scraping Subject: {subject_name} =====")

        total_pages = get_total_pages(session, base_url)

        output_file = f"preply_tutors_{subject_name}.csv"

        scrape_all_pages(
            base_url=base_url,
            total_pages=total_pages,
            start_page=1,
            output_file=output_file
        )