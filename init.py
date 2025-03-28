import sqlite3
import requests
import time
import logging
import re
from typing import List, Optional

# Setup logging configuration
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s: %(message)s')

# Constants for maintainability.
DB_NAME = 'bible.db'
PLACEHOLDER = '###'
TRANSLATION = 'ESV'
HOURLY_LIMIT = 1000
DAILY_LIMIT = 5000

# Dictionary mapping each book to a list of verse counts for each chapter.
# (These counts follow the standard divisions found in many NKJV/ESV Bibles.)
bible_structure = {
    "Genesis": [31,25,24,26,32,22,24,22,29,32,32,20,18,24,21,16,27,33,38,18,34,24,20,67,34,35,46,22,35,43,55,32,20,31,29,43,36,30,23,23,57,38,34,34,28,34,31,22,33,26],
    "Exodus": [22,25,22,31,23,30,25,32,35,29,10,51,22,31,27,36,16,27,25,26,36,31,33,18,40,37,21,43,46,38,18,35,23,35,35,38,29,31,43,38],
    "Leviticus": [17,16,17,35,19,30,38,36,24,20,47,8,59,57,33,34,26,20,30,37,10,51,12,15,15,27,18,28,23,10,20,38,17,8,36,24,22,44,23,55,46,34],
    "Numbers": [54,34,51,49,31,27,89,26,23,36,35,16,33,45,41,50,13,32,22,29,35,41,30,25,18,65,23,31,39,17,54,42,56,29,34,13,33,34,28,31,47,26],
    "Deuteronomy": [46,37,29,49,33,25,26,20,29,22,32,32,18,29,23,22,20,22,21,20,23,30,25,22,19,19,26,68,29,20,30,52,29,12],
    "Joshua": [18,24,17,24,15,27,26,35,27,43,23,24,33,15,63,10,18,28,51,9,45,34,16,33],
    "Judges": [36,23,31,24,31,40,25,35,57,18,40,15,25,20,20,31,13,31,30,48,25],
    "Ruth": [22,23,18,22],
    "1 Samuel": [28,36,21,22,12,21,17,22,27,27,15,25,23,52,35,23,58,30,24,42,15,23,29,22,44,25,12,25,11,31,13],
    "2 Samuel": [27,32,39,12,25,23,29,18,13,19,27,31,39,33,37,23,29,33,43,26,22,51,39,25],
    "1 Kings": [53,46,28,34,18,38,51,66,28,29,43,33,34,31,34,34,24,46,21,43,29,53],
    "2 Kings": [18,25,27,44,27,33,20,29,37,36,21,22,25,29,38,20,41,37,37,21,26,20,37,20,30],
    "1 Chronicles": [54,55,24,43,26,81,40,40,44,14,47,40,14,17,29,43,27,17,19,8,30,19,32,31,31,32,34,21,30,13,29,49,26,20,12,19,9,27,36,27,21,33,25,33,27,23,24,33,15,27],
    "2 Chronicles": [17,18,17,22,14,42,22,18,31,19,23,16,22,15,19,14,19,34,11,37,20,12,21,27,28,23,9,27,36,27,21,33,25,33,27,23,24,33,15,27],
    "Ezra": [11,70,13,24,17,22,28,36,15,44],
    "Nehemiah": [11,20,32,23,19,19,73,18,38,39,36,47,31],
    "Esther": [22,23,15,17,14,14,10,17,32,3],
    "Job": [22,13,26,21,27,30,21,22,35,22,20,25,28,22,35,22,16,21,29,29,34,30,17,25,6,14,23,28,25,31,40,22,33,37,16,33,24,41,30,24,34,17],
    "Psalms": [
        6,12,8,8,12,10,17,9,20,18,7,8,6,7,5,11,15,50,14,9,13,31,6,10,22,12,14,9,11,12,24,11,22,22,28,12,40,22,13,17,13,11,5,26,
        17,11,9,14,20,23,19,9,6,7,23,13,11,11,17,12,8,12,11,10,13,20,7,35,36,5,24,20,28,23,10,12,18,14,9,13,11,11,17,12,8,12,11
    ],
    "Proverbs": [33,22,35,27,23,35,27,36,18,32,31,28,25,35,33,33,28,24,29,30,31,29,35,34,28,28,27,28,27,33,31],
    "Ecclesiastes": [18,26,22,16,20,12,29,17,18,20,10,14],
    "Song of Solomon": [17,17,11,16,16,13,13,14],
    "Isaiah": [31,22,26,6,30,13,25,22,21,34,16,6,22,32,9,14,14,7,25,6,17,25,18,23,12,21,13,29,24,33,9,20,24,17,10,22,38,22,8,31,29,25,28,28,25,13,15,22,26,11,23,15,12,17,13,12,21,14,21,22,11,18,14,11,8,12,19,12,25,24],
    "Jeremiah": [19,37,25,31,31,30,34,22,26,25,23,17,27,22,21,21,27,23,15,18,14,30,40,10,38,24,22,17,32,24,40,44,26,22,19,32,21,28,18,16,18,22,13,30,5,28,7,47,39,46,64,34],
    "Lamentations": [22,22,66,22,22],
    "Ezekiel": [28,10,27,17,17,14,27,18,11,22,25,28,23,23,8,63,24,32,14,49,32,31,49,27,17,21,36,26,21,26,18,32,33,31,15,38,26,18,32,43,27,23,33,15,63,12,44],
    "Daniel": [21,49,30,37,31,28,28,27,27,21,45,13],
    "Hosea": [11,23,5,19,15,11,16,14,17,15,10,12,16,9],
    "Joel": [20,32,21],
    "Amos": [15,16,15,13,27,14,17,14,15],
    "Obadiah": [21],
    "Jonah": [17,10,10,11],
    "Micah": [16,13,12,13,15,16,20],
    "Nahum": [15,13,19],
    "Habakkuk": [17,20,19],
    "Zephaniah": [18,15,20],
    "Haggai": [15,23],
    "Zechariah": [21,13,10,14,11,15,14,23,17,12,17,14,9,21],
    "Malachi": [14,17,18,6],
    "Matthew": [25,23,17,25,48,34,29,34,38,42,30,50,58,36,27,33,26,40,42,31,37,47,30,57,29,34,26,28,28,25,13,15,22,46,39],
    "Mark": [45,28,35,41,43,56,37,38,50,52,33,44,37,72,47,20],
    "Luke": [80,52,38,44,39,49,50,56,62,42,54,59,35,35,32,31,37,43,48,47,38,71,56,53],
    "John": [51,25,36,54,47,71,53,59,41,42,57,50,38,31,27,33,26,40,42,31,25],
    "Acts": [26,47,26,37,42,15,60,40,43,48,30,25,52,28,41,40,34,28,41,38,40,30,35,27,27,32,44,31],
    "Romans": [32,29,31,25,21,23,25,39,33,21,36,21,14,23,33,27],
    "1 Corinthians": [31,16,23,21,13,20,40,13,27,33,34,31,13,40,58,24],
    "2 Corinthians": [24,17,18,18,21,18,16,24,15,18,33,21,14],
    "Galatians": [24,21,29,31,26,18],
    "Ephesians": [23,22,21,32,33,24],
    "Philippians": [30,30,21,23],
    "Colossians": [29,23,25,18],
    "1 Thessalonians": [10,20,13,18,28],
    "2 Thessalonians": [12,17,18],
    "1 Timothy": [20,15,16,16,25,21],
    "2 Timothy": [18,26,17,22],
    "Titus": [16,15,15],
    "Philemon": [25],
    "Hebrews": [14,18,19,16,14,20,28,13,28,39,40,29,25],
    "James": [27,26,18,17,20],
    "1 Peter": [25,25,22,19,14],
    "2 Peter": [21,22,18],
    "1 John": [10,29,24,21,21],
    "2 John": [13],
    "3 John": [15],
    "Jude": [25],
    "Revelation": [20,29,22,11,14,17,17,13,21,11,19,17,18,20,8,21,18,24,21,15,27,21]
}

def create_database() -> None:
    with sqlite3.connect(DB_NAME) as conn:
        cursor = conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS books (
                book_id INTEGER PRIMARY KEY,
                name TEXT UNIQUE NOT NULL
            )
        ''')
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS chapters (
                chapter_id INTEGER PRIMARY KEY,
                book_id INTEGER NOT NULL,
                chapter_number INTEGER NOT NULL,
                verse_count INTEGER NOT NULL,
                FOREIGN KEY (book_id) REFERENCES books(book_id)
            )
        ''')
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS verses (
                verse_id INTEGER PRIMARY KEY,
                chapter_id INTEGER NOT NULL,
                verse_number INTEGER NOT NULL,
                translation TEXT NOT NULL,
                text TEXT,
                word_count INTEGER,
                FOREIGN KEY (chapter_id) REFERENCES chapters(chapter_id)
            )
        ''')
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS api_tracking (
                id INTEGER PRIMARY KEY,
                request_count INTEGER DEFAULT 0,
                last_request_hour INTEGER DEFAULT 0,
                last_request_day INTEGER DEFAULT 0
            )
        ''')
        cursor.execute('INSERT OR IGNORE INTO api_tracking (id) VALUES (1)')
        conn.commit()
    logging.info("Database and tables created successfully.")

def populate_books_and_chapters() -> None:
    with sqlite3.connect(DB_NAME) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM books")
        if cursor.fetchone()[0] > 0:
            logging.info("Books already populated. Skipping this step.")
            return

        for book, chapters in bible_structure.items():
            cursor.execute("INSERT INTO books (name) VALUES (?)", (book,))
            book_id = cursor.lastrowid
            for idx, verse_count in enumerate(chapters, start=1):
                cursor.execute(
                    "INSERT INTO chapters (book_id, chapter_number, verse_count) VALUES (?, ?, ?)",
                    (book_id, idx, verse_count)
                )
        conn.commit()
    logging.info("Books and chapters populated successfully.")

def bootstrap_verses() -> None:
    with sqlite3.connect(DB_NAME) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM books")
        if cursor.fetchone()[0] == 0:
            logging.error("No books found in the database. Please populate books and chapters first.")
            return

        cursor.execute("""
            SELECT chapters.chapter_id, books.name, chapters.chapter_number, chapters.verse_count
            FROM chapters 
            JOIN books ON chapters.book_id = books.book_id
        """)
        chapters = cursor.fetchall()

        logging.info("Ensuring all chapters have contiguous placeholder verses...")
        missing_records = []
        for chapter_id, book_name, chapter_number, verse_count in chapters:
            cursor.execute("SELECT verse_number FROM verses WHERE chapter_id = ?", (chapter_id,))
            existing = {row[0] for row in cursor.fetchall()}
            missing = [i for i in range(1, verse_count + 1) if i not in existing]
            if missing:
                missing_records.extend([(chapter_id, verse, PLACEHOLDER) for verse in missing])
                logging.info(f"Chapter {book_name} {chapter_number} is missing verses: {missing}")

        if missing_records:
            cursor.executemany(
                "INSERT INTO verses (chapter_id, verse_number, translation) VALUES (?, ?, ?)",
                missing_records
            )
            logging.info(f"Inserted {len(missing_records)} missing placeholder verses.")
        else:
            logging.info("No missing placeholder verses found.")
        conn.commit()
    logging.info("Bootstrap complete: All chapters now have contiguous placeholder verses.")

def check_rate_limit(conn: sqlite3.Connection) -> bool:
    current_hour = int(time.time() // 3600)
    current_day = int(time.time() // 86400)
    cursor = conn.cursor()
    cursor.execute('SELECT request_count, last_request_hour, last_request_day FROM api_tracking WHERE id = 1')
    request_count, last_hour, last_day = cursor.fetchone()

    if current_day != last_day:
        cursor.execute('UPDATE api_tracking SET request_count = 0, last_request_day = ?, last_request_hour = ?',
                       (current_day, current_hour))
        request_count = 0
    elif current_hour != last_hour:
        cursor.execute('UPDATE api_tracking SET last_request_hour = ?', (current_hour,))

    if request_count >= DAILY_LIMIT:
        logging.warning("Daily API request limit reached. Please try again tomorrow.")
        return False
    elif request_count >= HOURLY_LIMIT and current_hour == last_hour:
        logging.warning("Hourly API request limit reached. Please try again later.")
        return False

    cursor.execute('UPDATE api_tracking SET request_count = request_count + 1 WHERE id = 1')
    conn.commit()
    return True

# Create a session to reuse HTTP connections for performance.
session = requests.Session()

def fetch_verses_text(book_name: str, chapter_number: int, verse_start: int, verse_end: int, api_key: str, conn: sqlite3.Connection) -> Optional[List[str]]:
    # If rate limit is reached, return None to signal we need to wait.
    if not check_rate_limit(conn):
        return None
    endpoint = "https://api.esv.org/v3/passage/text/"
    headers = {"Authorization": f"Token {api_key}"}
    params = {
        "q": f"{book_name} {chapter_number}:{verse_start}-{verse_end}",
        "include-footnotes": "false",
        "include-headings": "false",
        "include-verse-numbers": "true",
        "include-short-copyright": "false"
    }
    try:
        response = session.get(endpoint, headers=headers, params=params)
        if response.status_code == 200:
            data = response.json()
            full_text = data['passages'][0].strip()
            
            # Remove header if present.
            lines = full_text.splitlines()
            if lines and not re.match(r'^\s*\[', lines[0]):
                lines = lines[1:]
            cleaned_text = "\n".join(lines).strip()
            
            # Split the text into verse segments based on leading bracketed numbers.
            verse_segments = re.split(r'(?=\[\d+\])', cleaned_text)
            verse_texts = []
            for segment in verse_segments:
                segment = segment.strip()
                if not segment:
                    continue
                # Remove leading verse indicator (e.g. "[35]")
                cleaned_line = re.sub(r'^\[\d+\]\s*', '', segment)
                # Remove trailing " (ESV)" if present.
                cleaned_line = re.sub(r'\s*\(ESV\)$', '', cleaned_line)
                # Collapse any internal newlines/extra whitespace.
                cleaned_line = re.sub(r'\s+', ' ', cleaned_line).strip()
                verse_texts.append(cleaned_line)
            return verse_texts
        else:
            logging.error(f"Error fetching {book_name} {chapter_number}:{verse_start}-{verse_end}: {response.status_code}")
            return []
    except Exception as e:
        logging.error(f"Exception occurred: {e}")
        return []

def populate_translation(api_key: str) -> None:
    while True:
        with sqlite3.connect(DB_NAME) as conn:
            cursor = conn.cursor()
            # Check for remaining verses.
            cursor.execute("SELECT COUNT(*) FROM verses WHERE translation = ?", (PLACEHOLDER,))
            remaining = cursor.fetchone()[0]
            if remaining == 0:
                logging.info("All verses have been fetched and updated.")
                break

            # Compute total verse count per book.
            book_totals = {}
            cursor.execute("""
                SELECT b.name, SUM(c.verse_count)
                FROM chapters c
                JOIN books b ON c.book_id = b.book_id
                GROUP BY b.name
            """)
            for book_name, total_count in cursor.fetchall():
                book_totals[book_name] = total_count

            # Select chapters with placeholder verses.
            cursor.execute("""
                SELECT b.name, c.chapter_number, c.verse_count, c.chapter_id
                FROM chapters c
                JOIN books b ON c.book_id = b.book_id
                WHERE EXISTS (
                    SELECT 1 FROM verses v
                    WHERE v.chapter_id = c.chapter_id AND v.translation = ?
                )
            """, (PLACEHOLDER,))
            chapters = cursor.fetchall()
            if not chapters:
                logging.info("No chapters with placeholders found. Exiting.")
                break

            for book_name, chapter_number, verse_count, chapter_id in chapters:
                total_in_book = book_totals.get(book_name, verse_count)
                half_book_limit = max(total_in_book // 2, 1)
                batch_limit = min(500, half_book_limit)

                for start_verse in range(1, verse_count + 1, batch_limit):
                    end_verse = min(start_verse + batch_limit - 1, verse_count)
                    passages = fetch_verses_text(book_name, chapter_number, start_verse, end_verse, api_key, conn)
                    
                    # If passages is None, the rate limit has been reached.
                    if passages is None:
                        # Calculate time remaining until next hour.
                        current_time = time.time()
                        next_hour = ((current_time // 3600) + 1) * 3600
                        wait_time = next_hour - current_time
                        logging.info(f"Rate limit reached. Pausing processing for {wait_time:.0f} seconds until next hour.")
                        time.sleep(wait_time)
                        # After waiting, break out of the inner loop to re-check rate limits.
                        break

                    if not passages:
                        logging.info(f"No passages returned for {book_name} {chapter_number}:{start_verse}-{end_verse}. Skipping.")
                        continue

                    updated_count = 0
                    for verse_num, passage in enumerate(passages, start=start_verse):
                        passage = passage.strip()
                        word_count = len(passage.split())
                        cursor.execute("""
                            UPDATE verses 
                            SET text = ?, word_count = ?, translation = ?
                            WHERE chapter_id = ? AND verse_number = ? AND translation = ?
                        """, (passage, word_count, TRANSLATION, chapter_id, verse_num, PLACEHOLDER))
                        updated_count += 1

                    logging.info(f"API call: Fetched and updated {updated_count} verses for {book_name} {chapter_number} (verses {start_verse}-{end_verse}).")
                    time.sleep(1)
            conn.commit()
        # Sleep before checking for more placeholders.
        time.sleep(30)

def main() -> None:
    api_key = input("Enter your API Key for fetching Bible passages: ").strip()
    create_database()
    populate_books_and_chapters()  # Populate the Bible structure with accurate verse counts.
    bootstrap_verses()             # Insert placeholder verses based on stored verse counts.
    populate_translation(api_key)  # Fetch and update verse texts while respecting batch limits.

if __name__ == '__main__':
    main()