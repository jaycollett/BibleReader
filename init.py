import sqlite3
import requests
import time
import logging
import re
import json
import argparse
from typing import List, Optional, Dict, Any, Union

# Setup logging configuration
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s: %(message)s')

# Constants for maintainability.
DB_NAME = 'bible.db'
PLACEHOLDER = '###'

# Rate limits per translation
RATE_LIMITS = {
    'ESV': {
        'minute': 60,
        'hourly': 1000,
        'daily': 5000
    },
    'KJV': {
        'hourly': 500,  
        'daily': 2000
    },
    'NIV': {
        'hourly': 100,
        'daily': 1000
    }
}

# Bible translations data
TRANSLATIONS = {
    'ESV': {
        'name': 'English Standard Version',
        'api_endpoint': 'https://api.esv.org/v3/passage/text/',
        'auth_header': 'Token',  # Will be combined with the API key
        'params': {
            "include-footnotes": "false",
            "include-headings": "false",
            "include-verse-numbers": "true",
            "include-short-copyright": "false"
        }
    },
    'KJV': {
        'name': 'King James Version',
        'api_endpoint': 'https://api.example.com/kjv/text',
        'auth_header': 'ApiKey',
        'params': {
            "format": "json",
            "include_verses": "true",
            "include_footnotes": "false"
        }
    },
    'NIV': {
        'name': 'New International Version',
        'api_endpoint': 'https://api.example.com/niv/passages',
        'auth_header': 'Bearer',
        'params': {
            "format": "json",
            "show_notes": "false",
            "include_headings": "false"
        }
    }
}

# Verified Bible structure data (chapter/verse counts)
bible_structure = {
    "Genesis": [31,25,24,26,32,22,24,22,29,32,
                32,20,18,24,21,16,27,33,38,18,
                34,24,20,67,34,35,46,22,35,43,
                55,32,20,31,29,43,36,30,23,23,
                57,38,34,34,28,34,31,22,33,26],
    "Exodus": [22,25,22,31,23,30,25,32,35,29,
               10,51,22,31,27,36,16,27,25,26,
               36,31,33,18,40,37,21,43,46,38,
               18,35,23,35,35,38,29,31,43,38],
    "Leviticus": [17,16,17,35,19,30,38,36,24,20,
                  47,8,59,57,33,34,26,20,30,37,
                  10,51,12,15,15,27,18],
    "Numbers": [54,34,51,49,31,27,89,26,23,36,
                35,16,33,45,41,50,13,32,22,29,
                35,41,30,25,18,65,23,31,39,17,
                54,42,56,29,34,13],
    "Deuteronomy": [46,37,29,49,33,25,26,20,29,22,
                    32,32,18,29,23,22,20,22,21,20,
                    23,30,25,22,19,19,26,68,29,20,
                    30,52,29,12],
    "Joshua": [18,24,17,24,15,27,26,35,27,43,
               23,24,33,15,63,10,18,28,51,9,
               45,34,16,33],
    "Judges": [36,23,31,24,31,40,25,35,57,18,
               40,15,25,20,20,31,13,31,30,48,25],
    "Ruth": [22,23,18,22],
    "1 Samuel": [28,36,21,22,12,21,17,22,27,27,
                  15,25,23,52,35,23,58,30,24,42,
                  15,23,29,22,44,25,12,25,11,31,13],
    "2 Samuel": [27,32,39,12,25,23,29,18,13,19,
                  27,31,39,33,37,23,29,33,43,26,
                  22,51,39,25],
    "1 Kings": [53,46,28,34,18,38,51,66,28,29,
                43,33,34,31,34,34,24,46,21,43,
                29,53],
    "2 Kings": [18,25,27,44,27,33,20,29,37,36,
                21,22,25,29,38,20,41,37,37,21,
                26,20,37,20,30],
    "1 Chronicles": [54,55,24,43,26,81,40,40,44,14,
                     47,40,14,17,29,43,27,17,19,8,
                     30,19,32,31,31,32,34,21,30],
    "2 Chronicles": [17,18,17,22,14,42,22,18,31,19,
                     23,16,22,15,19,14,19,34,11,37,
                     20,12,21,27,28,23,9,27,36,27,
                     21,33,25,33,27,23],
    "Ezra": [11,70,13,24,17,22,28,36,15,44],
    "Nehemiah": [11,20,32,23,19,19,73,18,38,39,36,47,31],
    "Esther": [22,23,15,17,14,14,10,17,32,3],
    "Job": [22,13,26,21,27,30,21,22,35,22,
            20,25,28,22,35,22,16,21,29,29,
            34,30,17,25,6,14,23,28,25,31,
            40,22,33,37,16,33,24,41,30,24,
            34,17],
    "Psalms": [6,12,8,8,12,10,17,9,20,18,
               7,8,6,7,5,11,15,50,14,9,
               13,31,6,10,22,12,14,9,11,12,
               24,11,22,22,28,12,40,22,13,17,
               13,11,5,26,17,11,9,14,20,23,
               19,9,6,7,23,13,11,11,17,12,
               8,12,11,10,13,20,7,35,36,5,
               24,20,28,23,10,12,18,14,9,13,
               11,11,17,12,8,12,11],
    "Proverbs": [33,22,35,27,23,35,27,36,18,32,
                 31,28,25,35,33,33,28,24,29,30,
                 31,29,35,34,28,28,27,28,27,33,31],
    "Ecclesiastes": [18,26,22,16,20,12,29,17,18,20,10,14],
    "Song of Solomon": [17,17,11,16,16,13,13,14],
    "Isaiah": [31,22,26,6,30,13,25,22,21,34,
               16,6,22,32,9,14,14,7,25,6,
               17,25,18,23,12,21,13,29,24,33,
               9,20,24,17,10,22,38,22,8,31,
               29,25,28,28,25,13,15,22,26,11,
               23,15,12,17,13,12,21,14,21,22,
               11,18,14,11,8,12,19,12,25,24],
    "Jeremiah": [19,37,25,31,31,30,34,22,26,25,
                 23,17,27,22,21,21,27,23,15,18,
                 14,30,40,10,38,24,22,17,32,24,
                 40,44,26,22,19,32,21,28,18,16,
                 18,22,13,30,5,28,7,47,39,46,
                 64,34],
    "Lamentations": [22,22,66,22,22],
    "Ezekiel": [28,10,27,17,17,14,27,18,11,22,
                25,28,23,23,8,63,24,32,14,49,
                32,31,49,27,17,21,36,26,21,26,
                18,32,33,31,15,38,26,18,32,43,
                27,23,33,15,63,12,44],
    "Daniel": [21,49,30,37,31,28,28,27,27,21,
               45,13],
    "Hosea": [11,23,5,19,15,11,16,14,17,15,
              10,12,16,9],
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
    "Matthew": [25,23,17,25,48,34,29,34,38,42,
                30,50,58,36,27,33,26,40,42,31,
                37,47,30,57,29,34,26,28],
    "Mark": [45,28,35,41,43,56,37,38,50,52,
             33,44,37,72,47,20],
    "Luke": [80,52,38,44,39,49,50,56,62,42,
             54,59,35,35,32,31,37,43,48,47,
             38,71,56,53],
    "John": [51,25,36,54,47,71,53,59,41,42,
             57,50,38,31,27,33,26,40,42,31,25],
    "Acts": [26,47,26,37,42,15,60,40,43,48,
             30,25,52,28,41,40,34,28,41,38,
             40,30,35,27,27,32,44,31],
    "Romans": [32,29,31,25,21,23,25,39,33,21,
               36,21,14,23,33,27],
    "1 Corinthians": [31,16,23,21,13,20,40,13,27,33,
                      34,31,13,40,58,24],
    "2 Corinthians": [24,17,18,18,21,18,16,24,15,18,
                      33,21,14],
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
    "Hebrews": [14,18,19,16,14,20,28,13,28,39,
                40,29,25],
    "James": [27,26,18,17,20],
    "1 Peter": [25,25,22,19,14],
    "2 Peter": [21,22,18],
    "1 John": [10,29,24,21,21],
    "2 John": [13],
    "3 John": [15],
    "Jude": [25],
    "Revelation": [20,29,22,11,14,17,17,13,21,11,
                   19,17,18,20,8,21,18,24,21,15,27,21]
}

# Translation-specific Bible structure and omitted verses
TRANSLATION_DATA = {
    'ESV': {
        'structure': bible_structure,  # Using the default structure
        'omitted_verses': {
            "Matthew": {
                17: [21],
                18: [11]
            },
            "Mark": {
                9: [44, 46],
                11: [26],
                15: [28]
            },
            "Luke": {
                17: [36]
            },
            "John": {
                7: list(range(53, 63))  # Verses 53 through 62 are omitted
            },
            "Acts": {
                8: [37]
            },
            "Romans": {
                16: [24]
            },
            "1 Corinthians": {
                14: [34]
            },
            "1 Timothy": {
                3: [16],
                4: [9]
            },
            "Titus": {
                2: [15]
            },
            "2 Peter": {
                1: [20]
            },
            "2 John": {
                1: [9]
            },
            "Revelation": {
                22: [19]
            }
        }
    },
    'KJV': {
        'structure': bible_structure,  # Using the default structure
        'omitted_verses': {}  # KJV includes all verses
    },
    'NIV': {
        'structure': bible_structure,  # Using the default structure
        'omitted_verses': {
            "Matthew": {
                17: [21],
                18: [11]
            },
            "Mark": {
                7: [16],
                9: [44, 46],
                11: [26],
                15: [28]
            },
            "Luke": {
                17: [36],
                23: [17]
            },
            "John": {
                5: [4]
            },
            "Acts": {
                8: [37],
                15: [34],
                24: [7],
                28: [29]
            },
            "Romans": {
                16: [24]
            }
        }
    }
}


def is_omitted(book: str, chapter: int, verse: int, translation: str) -> bool:
    """Check if a verse is omitted in the specified translation."""
    if translation not in TRANSLATION_DATA:
        return False
    omitted_verses = TRANSLATION_DATA[translation].get('omitted_verses', {})
    return verse in omitted_verses.get(book, {}).get(chapter, [])


def create_database() -> None:
    with sqlite3.connect(DB_NAME) as conn:
        cursor = conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS translations (
                translation_id INTEGER PRIMARY KEY,
                abbreviation TEXT UNIQUE NOT NULL,
                name TEXT NOT NULL,
                metadata JSON
            )
        ''')
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS books (
                book_id INTEGER PRIMARY KEY,
                translation_id INTEGER NOT NULL,
                name TEXT NOT NULL,
                metadata JSON,
                FOREIGN KEY (translation_id) REFERENCES translations(translation_id),
                UNIQUE(translation_id, name)
            )
        ''')
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS chapters (
                chapter_id INTEGER PRIMARY KEY,
                book_id INTEGER NOT NULL,
                chapter_number INTEGER NOT NULL,
                verse_count INTEGER NOT NULL,
                metadata JSON,
                FOREIGN KEY (book_id) REFERENCES books(book_id)
            )
        ''')
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS verses (
                verse_id INTEGER PRIMARY KEY,
                chapter_id INTEGER NOT NULL,
                verse_number INTEGER NOT NULL,
                text TEXT,
                word_count INTEGER,
                metadata JSON,
                FOREIGN KEY (chapter_id) REFERENCES chapters(chapter_id)
            )
        ''')
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS api_tracking (
                id INTEGER PRIMARY KEY,
                translation_id INTEGER NOT NULL,
                request_count INTEGER DEFAULT 0,
                last_request_hour INTEGER DEFAULT 0,
                last_request_day INTEGER DEFAULT 0,
                last_request_minute INTEGER DEFAULT 0,
                minute_request_count INTEGER DEFAULT 0,
                FOREIGN KEY (translation_id) REFERENCES translations(translation_id),
                UNIQUE(translation_id)
            )
        ''')
        conn.commit()
    logging.info("Database and tables created successfully.")

def register_translation(translation: str) -> Optional[int]:
    """Register a single translation in the database and return its ID.
    
    Args:
        translation: The translation code/abbreviation to register
        
    Returns:
        The translation_id if successful, None otherwise
    """
    if translation not in TRANSLATIONS:
        logging.error(f"Translation {translation} is not defined in TRANSLATIONS.")
        return None
        
    with sqlite3.connect(DB_NAME) as conn:
        cursor = conn.cursor()
        
        # Check if translation already exists
        cursor.execute("SELECT translation_id FROM translations WHERE abbreviation = ?", (translation,))
        result = cursor.fetchone()
        if result:
            return result[0]
            
        # Add the translation
        cursor.execute(
            "INSERT INTO translations (abbreviation, name) VALUES (?, ?)",
            (translation, TRANSLATIONS[translation]['name'])
        )
        translation_id = cursor.lastrowid
        conn.commit()
        logging.info(f"Translation {translation} registered with ID {translation_id}")
        return translation_id

def populate_books_and_chapters() -> None:
    with sqlite3.connect(DB_NAME) as conn:
        cursor = conn.cursor()
        
        # Make sure translations are populated first
        cursor.execute("SELECT COUNT(*) FROM translations")
        if cursor.fetchone()[0] == 0:
            logging.error("No translations found in the database. Please populate translations first.")
            return
        
        # Get all translations
        cursor.execute("SELECT translation_id, abbreviation FROM translations")
        translations = cursor.fetchall()
        
        for translation_id, translation_code in translations:
            # Check if books for this translation are already populated
            cursor.execute("SELECT COUNT(*) FROM books WHERE translation_id = ?", (translation_id,))
            if cursor.fetchone()[0] > 0:
                logging.info(f"Books for {translation_code} already populated. Skipping.")
                continue
            
            # Get translation-specific structure
            if translation_code not in TRANSLATION_DATA:
                logging.warning(f"No structure data for {translation_code}. Skipping.")
                continue
                
            book_structure = TRANSLATION_DATA[translation_code].get('structure', {})
            if not book_structure:
                logging.warning(f"Empty structure for {translation_code}. Skipping.")
                continue
                
            # Populate books and chapters for this translation
            for book, chapters in book_structure.items():
                cursor.execute(
                    "INSERT INTO books (translation_id, name) VALUES (?, ?)", 
                    (translation_id, book)
                )
                book_id = cursor.lastrowid
                for idx, verse_count in enumerate(chapters, start=1):
                    cursor.execute(
                        "INSERT INTO chapters (book_id, chapter_number, verse_count) VALUES (?, ?, ?)",
                        (book_id, idx, verse_count)
                    )
            logging.info(f"Books and chapters for {translation_code} populated successfully.")
        
        conn.commit()
    logging.info("Books and chapters population complete for all translations.")

def bootstrap_verses(translation: str) -> None:
    with sqlite3.connect(DB_NAME) as conn:
        cursor = conn.cursor()
        
        # Get translation_id for the specified translation
        cursor.execute("SELECT translation_id FROM translations WHERE abbreviation = ?", (translation,))
        result = cursor.fetchone()
        if not result:
            logging.error(f"Translation {translation} not found in the database. Please populate translations first.")
            return
        translation_id = result[0]
        
        # Check if books exist for this translation
        cursor.execute("SELECT COUNT(*) FROM books WHERE translation_id = ?", (translation_id,))
        if cursor.fetchone()[0] == 0:
            logging.error(f"No books found for {translation}. Please populate books and chapters first.")
            return

        # Get all chapters for this translation
        cursor.execute("""
            SELECT chapters.chapter_id, books.name, chapters.chapter_number, chapters.verse_count
            FROM chapters 
            JOIN books ON chapters.book_id = books.book_id
            WHERE books.translation_id = ?
        """, (translation_id,))
        chapters = cursor.fetchall()

        logging.info(f"Ensuring all chapters have contiguous placeholder verses for {translation}...")
        missing_records = []
        for chapter_id, book_name, chapter_number, verse_count in chapters:
            cursor.execute("SELECT verse_number FROM verses WHERE chapter_id = ?", (chapter_id,))
            existing = {row[0] for row in cursor.fetchall()}
            missing = [i for i in range(1, verse_count + 1) if i not in existing]
            if missing:
                for v in missing:
                    if is_omitted(book_name, chapter_number, v, translation):
                        missing_records.append((chapter_id, v, f"omitted in {translation}"))
                    else:
                        missing_records.append((chapter_id, v, PLACEHOLDER))
                logging.info(f"Chapter {book_name} {chapter_number} is missing verses: {missing}")

        if missing_records:
            cursor.executemany(
                "INSERT INTO verses (chapter_id, verse_number, text) VALUES (?, ?, ?)",
                missing_records
            )
            logging.info(f"Inserted {len(missing_records)} missing placeholder verses for {translation}.")
        else:
            logging.info(f"No missing placeholder verses found for {translation}.")
        conn.commit()
    logging.info(f"Bootstrap complete: All chapters now have contiguous placeholder verses for {translation}.")


def check_rate_limit(conn: sqlite3.Connection, translation: str) -> bool:
    """Check API rate limits for a specific translation."""
    if translation not in RATE_LIMITS:
        logging.error(f"No rate limits defined for {translation}.")
        return False
    
    limits = RATE_LIMITS[translation]
    minute_limit = limits.get('minute', float('inf'))
    hourly_limit = limits.get('hourly', float('inf'))
    daily_limit = limits.get('daily', float('inf'))
    
    current_minute = int(time.time() // 60)
    current_hour = int(time.time() // 3600)
    current_day = int(time.time() // 86400)
    cursor = conn.cursor()
    
    # Get translation_id
    cursor.execute('SELECT translation_id FROM translations WHERE abbreviation = ?', (translation,))
    result = cursor.fetchone()
    if not result:
        logging.error(f"Translation {translation} not found in the database.")
        return False
    translation_id = result[0]
    
    # Check if we need to alter the api_tracking table to add the minute tracking column
    cursor.execute("PRAGMA table_info(api_tracking)")
    columns = [col[1] for col in cursor.fetchall()]
    if 'last_request_minute' not in columns:
        cursor.execute('ALTER TABLE api_tracking ADD COLUMN last_request_minute INTEGER DEFAULT 0')
        cursor.execute('ALTER TABLE api_tracking ADD COLUMN minute_request_count INTEGER DEFAULT 0')
        conn.commit()
        logging.info("Added minute-based rate limit tracking columns to api_tracking table")
    
    # Check if there is a record for this translation
    cursor.execute('SELECT COUNT(*) FROM api_tracking WHERE translation_id = ?', (translation_id,))
    if cursor.fetchone()[0] == 0:
        # Insert a new record for this translation
        cursor.execute(
            'INSERT INTO api_tracking (translation_id, request_count, last_request_hour, last_request_day, last_request_minute, minute_request_count) VALUES (?, 0, ?, ?, ?, 0)',
            (translation_id, current_hour, current_day, current_minute)
        )
        conn.commit()
        
    cursor.execute(
        'SELECT request_count, last_request_hour, last_request_day, minute_request_count, last_request_minute FROM api_tracking WHERE translation_id = ?',
        (translation_id,)
    )
    request_count, last_hour, last_day, minute_count, last_minute = cursor.fetchone()

    # Reset counters if time periods have changed
    if current_day != last_day:
        cursor.execute(
            'UPDATE api_tracking SET request_count = 0, last_request_day = ?, last_request_hour = ?, last_request_minute = ?, minute_request_count = 0 WHERE translation_id = ?',
            (current_day, current_hour, current_minute, translation_id)
        )
        request_count = 0
        minute_count = 0
    elif current_hour != last_hour:
        # Reset the hourly counter when the hour changes.
        cursor.execute(
            'UPDATE api_tracking SET request_count = 0, last_request_hour = ?, last_request_minute = ?, minute_request_count = 0 WHERE translation_id = ?',
            (current_hour, current_minute, translation_id)
        )
        request_count = 0
        minute_count = 0
    elif current_minute != last_minute:
        # Reset the minute counter when the minute changes.
        cursor.execute(
            'UPDATE api_tracking SET last_request_minute = ?, minute_request_count = 0 WHERE translation_id = ?',
            (current_minute, translation_id)
        )
        minute_count = 0

    # Check all rate limits
    if request_count >= daily_limit:
        logging.warning(f"Daily API request limit ({daily_limit}) reached for {translation}. Please try again tomorrow.")
        return False
    elif request_count >= hourly_limit:
        logging.warning(f"Hourly API request limit ({hourly_limit}) reached for {translation}. Please try again later.")
        return False
    elif minute_count >= minute_limit:
        logging.warning(f"Per-minute API request limit ({minute_limit}) reached for {translation}. Please try again in a minute.")
        return False

    # Increment all counters
    cursor.execute(
        'UPDATE api_tracking SET request_count = request_count + 1, minute_request_count = minute_request_count + 1 WHERE translation_id = ?',
        (translation_id,)
    )
    conn.commit()
    return True

# Create a session to reuse HTTP connections for performance.
session = requests.Session()

# Registry for translation-specific response processors
RESPONSE_PROCESSORS = {}

def register_response_processor(translation_code: str):
    """Decorator to register a translation-specific response processor."""
    def decorator(func):
        RESPONSE_PROCESSORS[translation_code] = func
        return func
    return decorator

@register_response_processor('ESV')
def process_esv_response(data: Dict[str, Any], translation: str) -> Dict[str, Any]:
    """Process ESV API response into a list of individual verse texts with metadata.
    
    ESV API response is structured with:
    - passages: List of text passages with verse numbers in brackets
    - passage_meta: Metadata about the passages
    - parsed: Parsed verse references
    - query: Original query text
    - canonical: Canonical reference
    
    Returns a dictionary containing:
    - texts: List of individual verse texts with verse indicators removed
    - metadata: Dictionary with verse IDs and other metadata from the API
    """
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
        # Remove trailing e.g. " (ESV)" if present.
        cleaned_line = re.sub(r'\s*\(' + translation + r'\)$', '', cleaned_line)
        # Collapse any internal newlines/extra whitespace.
        cleaned_line = re.sub(r'\s+', ' ', cleaned_line).strip()
        verse_texts.append(cleaned_line)
    
    # Extract metadata from the API response
    metadata = {
        'query': data.get('query'),
        'canonical': data.get('canonical'),
        'parsed': data.get('parsed')
    }
    
    # Extract passage metadata if available
    if 'passage_meta' in data and data['passage_meta']:
        passage_meta = data['passage_meta'][0]
        metadata['passage_meta'] = passage_meta
        
        # Extract verse IDs if available
        verse_ids = {}
        if 'parsed' in data and data['parsed']:
            parsed = data['parsed'][0]
            # Map the start and end verse IDs to the verses
            if len(parsed) >= 2:
                start_id, end_id = parsed
                verse_range = range(start_id, end_id + 1)
                verse_ids = {v+1: id for v, id in enumerate(verse_range)}
        metadata['verse_ids'] = verse_ids
    
    return {
        'texts': verse_texts,
        'metadata': metadata
    }

@register_response_processor('KJV')
def process_kjv_response(data: Dict[str, Any], translation: str) -> Dict[str, Any]:
    """Process KJV API response into a list of individual verse texts with metadata.
    
    KJV API response is structured with:
    - verses: Array of verse objects with 'text' and 'number' properties
    - reference: Complete reference information
    - book: Book information
    - chapter: Chapter information
    
    Returns a dictionary containing:
    - texts: List of individual verse texts
    - metadata: Dictionary with reference information
    """
    # Extract individual verses from the response
    verse_texts = []
    verse_ids = {}
    
    # The KJV API returns individual verses already separated
    for i, verse in enumerate(data['verses']):
        verse_number = verse['number']
        verse_text = verse['text']
        
        # Clean up the verse text
        verse_text = re.sub(r'\s+', ' ', verse_text).strip()
        verse_texts.append(verse_text)
        
        # Store verse ID if available
        if 'id' in verse:
            verse_ids[verse_number] = verse['id']
    
    # Extract metadata from the API response
    metadata = {
        'reference': data.get('reference'),
        'book': data.get('book'),
        'chapter': data.get('chapter'),
        'verse_ids': verse_ids
    }
    
    return {
        'texts': verse_texts,
        'metadata': metadata
    }

@register_response_processor('NIV')
def process_niv_response(data: Dict[str, Any], translation: str) -> Dict[str, Any]:
    """Process NIV API response into a list of individual verse texts with metadata.
    
    NIV API response is structured with:
    - content: HTML or JSON formatted content with verses
    - metadata: Reference and passage information
    - verses: Array of verse data with numbers and content
    
    Returns a dictionary containing:
    - texts: List of individual verse texts
    - metadata: Dictionary with reference information
    """
    # Extract individual verses from the response
    verse_texts = []
    metadata = {
        'passage': data.get('metadata', {}).get('passage'),
        'version': data.get('metadata', {}).get('version'),
        'copyright': data.get('metadata', {}).get('copyright')
    }
    
    # The NIV API returns verses in a nested structure
    verses = data.get('verses', [])
    for verse in verses:
        verse_text = verse.get('content', '')
        
        # Clean up the verse text - remove HTML tags if present
        verse_text = re.sub(r'<[^>]+>', '', verse_text)
        verse_text = re.sub(r'\s+', ' ', verse_text).strip()
        
        if verse_text:
            verse_texts.append(verse_text)
    
    return {
        'texts': verse_texts,
        'metadata': metadata
    }

# Registry for translation-specific fetchers
TRANSLATION_FETCHERS = {}

def register_translation_fetcher(translation_code: str):
    """Decorator to register a translation-specific fetch function."""
    def decorator(func):
        TRANSLATION_FETCHERS[translation_code] = func
        return func
    return decorator

@register_translation_fetcher('ESV')
def fetch_esv_verses(book_name: str, chapter_number: int, verse_start: int, verse_end: int, 
                   api_key: str, conn: sqlite3.Connection) -> Optional[Dict[str, Any]]:
    """Fetch ESV Bible verses with translation-specific handling.
    
    ESV API has the following characteristics:
    - Requires a token-based authentication in the header
    - Has hourly and daily rate limits (configured in RATE_LIMITS)
    - Response includes passages with verse numbers in brackets
    - Can fetch multiple verses in a single request
    - Returns passage metadata and canonical references
    
    Args:
        book_name: Name of the book (e.g., "Genesis")
        chapter_number: Chapter number
        verse_start: Starting verse number
        verse_end: Ending verse number
        api_key: The ESV API key
        conn: Database connection for tracking API usage
        
    Returns:
        Dictionary with verse texts and metadata, or None if failed
    """
    translation = 'ESV'
    
    # If rate limit is reached, return None to signal we need to wait.
    if not check_rate_limit(conn, translation):
        return None
    
    translation_config = TRANSLATIONS[translation]
    endpoint = translation_config['api_endpoint']
    auth_type = translation_config['auth_header']
    headers = {"Authorization": f"{auth_type} {api_key}"}
    
    # Start with API-specific parameters from config
    params = dict(translation_config.get('params', {}))
    # Add request-specific parameters
    params["q"] = f"{book_name} {chapter_number}:{verse_start}-{verse_end}"
    
    try:
        response = session.get(endpoint, headers=headers, params=params)
        if response.status_code == 200:
            data = response.json()
            # Use the processor for ESV
            return process_esv_response(data, translation)
        else:
            logging.error(f"Error fetching {book_name} {chapter_number}:{verse_start}-{verse_end} ({translation}): {response.status_code}")
            return None
    except Exception as e:
        logging.error(f"Exception occurred while fetching {translation} text: {e}")
        return None

@register_translation_fetcher('KJV')
def fetch_kjv_verses(book_name: str, chapter_number: int, verse_start: int, verse_end: int, 
                    api_key: str, conn: sqlite3.Connection) -> Optional[Dict[str, Any]]:
    """Fetch KJV Bible verses with translation-specific handling.
    
    KJV API has the following characteristics:
    - Uses API key as a query parameter (not in header)
    - Has more conservative rate limits than ESV
    - Returns individual verses directly as separate objects
    - Includes detailed metadata for each verse
    - Supports range requests with different URL structure
    
    Args:
        book_name: Name of the book (e.g., "Genesis")
        chapter_number: Chapter number
        verse_start: Starting verse number
        verse_end: Ending verse number
        api_key: The KJV API key
        conn: Database connection for tracking API usage
        
    Returns:
        Dictionary with verse texts and metadata, or None if failed
    """
    translation = 'KJV'
    
    # If rate limit is reached, return None to signal we need to wait.
    if not check_rate_limit(conn, translation):
        return None
    
    translation_config = TRANSLATIONS[translation]
    endpoint = translation_config['api_endpoint']
    auth_header = translation_config['auth_header']
    
    # For KJV API, the API key is passed as a query parameter, not in header
    params = dict(translation_config.get('params', {}))
    params["apiKey"] = api_key
    params["reference"] = f"{book_name} {chapter_number}:{verse_start}-{verse_end}"
    
    try:
        # KJV API doesn't use auth headers like ESV, so we don't need headers here
        response = session.get(endpoint, params=params)
        if response.status_code == 200:
            data = response.json()
            # Use the KJV-specific processor
            return process_kjv_response(data, translation)
        else:
            logging.error(f"Error fetching {book_name} {chapter_number}:{verse_start}-{verse_end} ({translation}): {response.status_code}")
            return None
    except Exception as e:
        logging.error(f"Exception occurred while fetching {translation} text: {e}")
        return None

@register_translation_fetcher('NIV')
def fetch_niv_verses(book_name: str, chapter_number: int, verse_start: int, verse_end: int, 
                    api_key: str, conn: sqlite3.Connection) -> Optional[Dict[str, Any]]:
    """Fetch NIV Bible verses with translation-specific handling.
    
    NIV API has the following characteristics:
    - Uses Bearer token authentication
    - Most restrictive rate limits of the translations
    - Returns complex data structure with verses in nested format
    - May include HTML formatting in verse text
    - Includes rich metadata about passages
    
    Args:
        book_name: Name of the book (e.g., "Genesis")
        chapter_number: Chapter number
        verse_start: Starting verse number
        verse_end: Ending verse number
        api_key: The NIV API key (access token)
        conn: Database connection for tracking API usage
        
    Returns:
        Dictionary with verse texts and metadata, or None if failed
    """
    translation = 'NIV'
    
    # NIV has the most restrictive rate limits
    if not check_rate_limit(conn, translation):
        return None
    
    translation_config = TRANSLATIONS[translation]
    endpoint = translation_config['api_endpoint']
    auth_type = translation_config['auth_header']
    
    # NIV API uses Bearer token authentication
    headers = {"Authorization": f"{auth_type} {api_key}"}
    
    # NIV API specific parameters
    params = dict(translation_config.get('params', {}))
    params["passage"] = f"{book_name} {chapter_number}:{verse_start}-{verse_end}"
    
    try:
        response = session.get(endpoint, headers=headers, params=params)
        if response.status_code == 200:
            data = response.json()
            # Use the NIV-specific processor
            return process_niv_response(data, translation)
        else:
            logging.error(f"Error fetching {book_name} {chapter_number}:{verse_start}-{verse_end} ({translation}): {response.status_code}")
            return None
    except Exception as e:
        logging.error(f"Exception occurred while fetching {translation} text: {e}")
        return None

def fetch_verses_text(book_name: str, chapter_number: int, verse_start: int, verse_end: int, 
                     translation: str, api_key: str, conn: sqlite3.Connection) -> Optional[Dict[str, Any]]:
    """Fetch verses text from the appropriate API based on the translation."""
    if translation not in TRANSLATIONS:
        logging.error(f"Translation {translation} not supported.")
        return None
    
    if translation not in TRANSLATION_FETCHERS:
        logging.error(f"No fetch function defined for {translation}.")
        return None
    
    # Use the registered translation-specific fetcher
    fetcher = TRANSLATION_FETCHERS[translation]
    return fetcher(book_name, chapter_number, verse_start, verse_end, api_key, conn)

def populate_translation(translation: str, api_key: str) -> None:
    """Populate verses for a specific translation using its API."""
    if translation not in TRANSLATIONS:
        logging.error(f"Translation {translation} not supported.")
        return

    while True:
        with sqlite3.connect(DB_NAME) as conn:
            cursor = conn.cursor()
            
            # Get translation_id
            cursor.execute("SELECT translation_id FROM translations WHERE abbreviation = ?", (translation,))
            result = cursor.fetchone()
            if not result:
                logging.error(f"Translation {translation} not found in database. Please populate translations first.")
                return
            translation_id = result[0]
            
            # Check for remaining verses.
            cursor.execute("""
                SELECT COUNT(*) FROM verses v
                JOIN chapters c ON v.chapter_id = c.chapter_id
                JOIN books b ON c.book_id = b.book_id
                WHERE b.translation_id = ? AND v.text = ?
            """, (translation_id, PLACEHOLDER))
            remaining = cursor.fetchone()[0]
            if remaining == 0:
                logging.info(f"All verses for {translation} have been fetched and updated.")
                break

            # Compute total verse count per book.
            book_totals = {}
            cursor.execute("""
                SELECT b.name, SUM(c.verse_count)
                FROM chapters c
                JOIN books b ON c.book_id = b.book_id
                WHERE b.translation_id = ?
                GROUP BY b.name
            """, (translation_id,))
            for book_name, total_count in cursor.fetchall():
                book_totals[book_name] = total_count

            # Select chapters with placeholder verses for this translation.
            cursor.execute("""
                SELECT b.name, c.chapter_number, c.verse_count, c.chapter_id, b.book_id
                FROM chapters c
                JOIN books b ON c.book_id = b.book_id
                WHERE b.translation_id = ? AND EXISTS (
                    SELECT 1 FROM verses v
                    WHERE v.chapter_id = c.chapter_id AND v.text = ?
                )
            """, (translation_id, PLACEHOLDER))
            chapters = cursor.fetchall()
            if not chapters:
                logging.info(f"No chapters with placeholders found for {translation}. Exiting.")
                break

            for book_name, chapter_number, verse_count, chapter_id, book_id in chapters:
                total_in_book = book_totals.get(book_name, verse_count)
                half_book_limit = max(total_in_book // 2, 1)
                batch_limit = min(500, half_book_limit)

                # Update chapter metadata if not done already
                cursor.execute("SELECT metadata FROM chapters WHERE chapter_id = ?", (chapter_id,))
                chapter_metadata = cursor.fetchone()[0]
                if not chapter_metadata:
                    try:
                        # Fetch the first verse to get chapter metadata
                        result = fetch_verses_text(book_name, chapter_number, 1, 1, translation, api_key, conn)
                        if result and 'metadata' in result:
                            chapter_meta = result['metadata']
                            # Extract only chapter-related metadata, not verse-specific info
                            chapter_metadata = {}
                            if 'passage_meta' in chapter_meta and chapter_meta['passage_meta']:
                                passage_meta = chapter_meta['passage_meta']
                                # Just store basic chapter info, not verse navigation
                                chapter_metadata = {
                                    'chapter_number': chapter_number,
                                    'canonical': chapter_meta.get('canonical', ''),
                                    'book_name': book_name,
                                    'verse_count': verse_count
                                }
                                # Add any chapter-level fields from passage_meta
                                if isinstance(passage_meta, dict):
                                    for key in ['chapter_start', 'chapter_end', 'book_start', 'book_end']:
                                        if key in passage_meta:
                                            chapter_metadata[key] = passage_meta[key]
                                
                                chapter_metadata_json = json.dumps(chapter_metadata)
                                cursor.execute("UPDATE chapters SET metadata = ? WHERE chapter_id = ?", 
                                              (chapter_metadata_json, chapter_id))
                                logging.info(f"Updated chapter metadata for {book_name} {chapter_number}")
                                
                                # Also update book metadata if not done already
                                cursor.execute("SELECT metadata FROM books WHERE book_id = ?", (book_id,))
                                book_metadata = cursor.fetchone()[0]
                                if not book_metadata and 'canonical' in chapter_meta:
                                    book_canonical = chapter_meta['canonical'].split(' ')[0]  # Extract book name
                                    book_metadata_json = json.dumps({'canonical': book_canonical})
                                    cursor.execute("UPDATE books SET metadata = ? WHERE book_id = ?", 
                                                  (book_metadata_json, book_id))
                                    logging.info(f"Updated book metadata for {book_name}")
                    except Exception as e:
                        logging.error(f"Error updating metadata for {book_name} {chapter_number}: {e}")

                for start_verse in range(1, verse_count + 1, batch_limit):
                    end_verse = min(start_verse + batch_limit - 1, verse_count)
                    result = fetch_verses_text(book_name, chapter_number, start_verse, end_verse, 
                                              translation, api_key, conn)
                    
                    # If result is None, the rate limit has been reached.
                    if result is None:
                        # Calculate time remaining until next hour.
                        current_time = time.time()
                        next_hour = ((current_time // 3600) + 1) * 3600
                        wait_time = next_hour - current_time
                        logging.info(f"Rate limit reached for {translation}. Pausing processing for {wait_time:.0f} seconds until next hour.")
                        time.sleep(wait_time)
                        # After waiting, break out of the inner loop to re-check rate limits.
                        break

                    if not result or 'texts' not in result or not result['texts']:
                        logging.info(f"No passages returned for {book_name} {chapter_number}:{start_verse}-{end_verse} ({translation}). Skipping.")
                        continue

                    updated_count = 0
                    verse_texts = result['texts']
                    metadata = result.get('metadata', {})
                    
                    for i, verse_text in enumerate(verse_texts):
                        verse_num = start_verse + i
                        verse_text = verse_text.strip()
                        word_count = len(verse_text.split())
                        
                        # Get verse-specific metadata if available
                        verse_metadata = None
                        if 'verse_ids' in metadata and verse_num in metadata['verse_ids']:
                            verse_id = metadata['verse_ids'][verse_num]
                            verse_metadata = json.dumps({'verse_id': verse_id})
                        
                        cursor.execute("""
                            UPDATE verses 
                            SET text = ?, word_count = ?, metadata = ?
                            WHERE chapter_id = ? AND verse_number = ?
                        """, (verse_text, word_count, verse_metadata, chapter_id, verse_num))
                        updated_count += 1

                    logging.info(f"API call: Fetched and updated {updated_count} verses for {book_name} {chapter_number} (verses {start_verse}-{end_verse}) in {translation}.")
                    # Sleep between API calls to ensure we don't exceed the per-minute rate limit
                    time.sleep(2)
            conn.commit()
        # Sleep before checking for more placeholders.
        time.sleep(30)

def process_translation(translation: str, api_key: str) -> None:
    """Process a specific Bible translation."""
    if translation not in TRANSLATIONS:
        logging.error(f"Translation {translation} is not supported.")
        print(f"Supported translations: {', '.join(TRANSLATIONS.keys())}")
        return
    
    if translation not in TRANSLATION_DATA:
        logging.error(f"No structure data defined for {translation}.")
        return
    
    # Create database and tables if they don't exist
    create_database()
    
    # Register this translation in the database
    translation_id = register_translation(translation)
    if not translation_id:
        logging.error(f"Failed to register translation {translation}.")
        return
    
    # Process the specific translation
    populate_books_and_chapters()                # This will only process translations that need it
    bootstrap_verses(translation)                # Insert placeholder verses for this translation
    populate_translation(translation, api_key)   # Fetch and update verse texts for this translation

def main() -> None:
    """Command-line entry point with support for arguments or interactive prompts."""
    parser = argparse.ArgumentParser(description='Bible Translation Text Fetcher')
    parser.add_argument('-t', '--translation', 
                        choices=list(TRANSLATIONS.keys()),
                        default='',
                        help='Bible translation to process')
    parser.add_argument('-k', '--key', 
                        help='API key for the translation service')
    parser.add_argument('-a', '--all', 
                        action='store_true',
                        help='Process all supported translations (requires API keys for all)')
    
    args = parser.parse_args()
    
    # If processing all translations
    if args.all:
        for trans in TRANSLATIONS.keys():
            key = input(f"Enter API Key for {trans} ({TRANSLATIONS[trans]['name']}): ").strip()
            if key:
                process_translation(trans, key)
            else:
                logging.warning(f"Skipping {trans} due to missing API key")
        return
    
    # Get translation from argument or prompt
    translation = args.translation
    if not translation:
        translation = input(f"Enter the Bible translation abbreviation [{', '.join(TRANSLATIONS.keys())}] (default: ESV): ").strip() or "ESV"
    
    # Get API key from argument or prompt
    api_key = args.key
    if not api_key:
        api_key = input(f"Enter your API Key for {translation} ({TRANSLATIONS[translation]['name']}): ").strip()
    
    if not api_key:
        logging.error("API key is required")
        return
        
    process_translation(translation, api_key)

if __name__ == '__main__':
    main()