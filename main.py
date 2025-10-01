import requests
from bs4 import BeautifulSoup
import csv
import time
import re
from urllib.parse import urljoin

# Ключові слова по секторах
SECTOR_KEYWORDS = {
    'agrarian': [
        'земл', 'сільськогосподар', 'фермер', 'підтримк', 'субсид',
        'експорт', 'хліб', 'зерно', 'фітосаніт', 'фітосанитар', 'квот', 'аграр', 'паї'
    ],
    'social': [
        'труд', 'зарплат', 'страхов', 'пенс', 'пенсій', 'праці',
        'охорон', 'соціал', 'внеск', 'штраф', 'відпустк'
    ],
    'corporate': [
        'подат', 'валют', 'корпоратив', 'управл', 'm&a', 'злит',
        'придб', 'борг', 'вій', 'ліміт', 'концерн'
    ]
}

SECTOR_LABELS = {
    'agrarian': 'Аграрний',
    'social': 'Соціальний',
    'corporate': 'Корпоративний',
    'other': 'Інший'
}

BASE_PORTAL = 'https://itd.rada.gov.ua'
PERIOD_LIST_URL = 'https://itd.rada.gov.ua/billInfo/Bills/period'

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (compatible; MVP-LegMonitor/1.0; +https://example.com)'
}

OUTPUT_CSV = 'bills_output.csv'


def get_soup(html):
    try:
        return BeautifulSoup(html, 'lxml')
    except Exception:
        return BeautifulSoup(html, 'html.parser')


def fetch_period_page():
    try:
        resp = requests.get(PERIOD_LIST_URL, headers=HEADERS, timeout=15)
        resp.raise_for_status()
        return resp.text
    except Exception as e:
        print(f"Помилка завантаження списку законопроектів: {e}")
        return None


def extract_bill_links(html, limit=50):
    soup = get_soup(html)
    links = []
    for a in soup.find_all('a', href=True):
        href = a['href']
        text = a.get_text(strip=True)
        if re.search(r'Bills/Details|BillInfo/Details|billInfo/Bill', href, re.I) or 'bill' in href.lower():
            links.append((text or 'No title', urljoin(BASE_PORTAL, href)))

    seen = set()
    uniq = []
    for t, u in links:
        if u not in seen:
            seen.add(u)
            uniq.append((t, u))
        if len(uniq) >= limit:
            break
    return uniq


def fetch_bill_page(url):
    try:
        resp = requests.get(url, headers=HEADERS, timeout=15)
        resp.raise_for_status()
        return resp.text
    except Exception as e:
        print(f"Не вдалось завантажити {url}: {e}")
        return None


def parse_bill(html, url):
    soup = get_soup(html)
    title = None
    if soup.title:
        title = soup.title.get_text(strip=True)
    if not title:
        h1 = soup.find(['h1', 'h2'])
        if h1:
            title = h1.get_text(strip=True)

    date_text = None
    text_blob = soup.get_text(separator=' ', strip=True)
    m = re.search(r'\d{2}\.\d{2}\.\d{4}|\d{4}-\d{2}-\d{2}', text_blob)
    if m:
        date_text = m.group(0)

    status = None
    if re.search(r'прийнят|прийнято|зареєстр', text_blob, re.I):
        status = 'registered/accepted'

    main_text = ''
    candidates = soup.find_all('div')
    longest = ''
    for c in candidates:
        t = c.get_text(separator=' ', strip=True)
        if len(t) > len(longest):
            longest = t
    main_text = longest or text_blob

    return {
        'url': url,
        'title': title or 'No title found',
        'published_date': date_text or '',
        'status': status or '',
        'full_text': main_text[:20000]
    }


def analyze_text_for_sectors(text):
    found = {k: [] for k in SECTOR_KEYWORDS.keys()}
    low = text.lower()
    for sector, kws in SECTOR_KEYWORDS.items():
        for kw in kws:
            if kw.lower() in low:
                found[sector].append(kw)
    matched = [s for s, lst in found.items() if lst]
    return matched, {s: list(set(found[s])) for s in found}


def compute_risk_score(matched, keywords_map, text):
    score = 0
    score += 2 * len(matched)
    for s in matched:
        score += len(keywords_map.get(s, []))
    if re.search(r'штраф|штрафи|санкц', text, re.I):
        score += 3
    if score > 10:
        score = 10
    return score


def summarize_text(text, max_chars=300):
    sentences = re.split(r'(?<=[.!?])\s+', text)
    if len(sentences) >= 2:
        s = ' '.join(sentences[:2])
    else:
        s = sentences[0] if sentences else ''
    s = s.strip()
    if len(s) > max_chars:
        s = s[:max_chars].rsplit(' ', 1)[0] + '...'
    return s


def build_row(parsed):
    matched, keywords_map = analyze_text_for_sectors(parsed['full_text'])
    risk = compute_risk_score(matched, keywords_map, parsed['full_text'])
    summary = summarize_text(parsed['full_text'])
    sector_main_eng = matched[0] if matched else 'other'
    sector_main = SECTOR_LABELS.get(sector_main_eng, 'Інший')
    row = {
        'bill_url': parsed['url'],
        'title': parsed['title'],
        'published_date': parsed['published_date'],
        'status': parsed['status'],
        'matched_sectors': ';'.join([SECTOR_LABELS.get(s, s) for s in matched]),
        'sector_main': sector_main,
        'keywords_found': ';'.join(sorted({kw for lst in keywords_map.values() for kw in lst})),
        'risk_score': risk,
        'summary': summary
    }
    return row


def save_rows_to_csv(rows, output=OUTPUT_CSV):
    fieldnames = [
        'bill_url', 'title', 'published_date', 'status',
        'matched_sectors', 'sector_main', 'keywords_found',
        'risk_score', 'summary'
    ]
    with open(output, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for r in rows:
            writer.writerow(r)
    print(f"Збережено {len(rows)} записів у {output}")


def main(limit=50):
    html = fetch_period_page()
    if not html:
        print("Немає HTML зі сторінки періоду — припиняємо.")
        return

    links = extract_bill_links(html, limit=limit)
    print(f"Знайдено близько {len(links)} потенційних посилань на законопроекти (heuristic).")

    rows = []
    for i, (title, url) in enumerate(links, start=1):
        print(f"[{i}/{len(links)}] Обробка: {title} - {url}")
        bill_html = fetch_bill_page(url)
        if not bill_html:
            continue
        parsed = parse_bill(bill_html, url)
        row = build_row(parsed)

        # ✅ ДОДАНО: Вивід посилання + сектор
        print(f"→ Сектор: {row['sector_main']} | Посилання: {row['bill_url']}")

        rows.append(row)
        time.sleep(0.8)

    if rows:
        rows = rows[:500]
        save_rows_to_csv(rows)
    else:
        print("Нічого не збережено — rows порожній.")


if __name__ == '__main__':
    main(limit=50)
