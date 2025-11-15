import os
import sys
import json
import re
from json import JSONDecodeError
from datetime import datetime

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, PROJECT_ROOT)

from ingestion_framework.utils.logger import get_logger
from ingestion_service.app.services.classification_service import process_data

logger = get_logger(__name__)

def repair_commas(s: str) -> str:
    # remove trailing commas before } or ]
    s = re.sub(r",\s*}", "}", s)
    s = re.sub(r",\s*]", "]", s)
    return s

def extract_json_objects(text: str):
    objs = []
    decoder = json.JSONDecoder()
    idx = 0
    length = len(text)
    while True:
        idx = text.find("{", idx)
        if idx == -1:
            break
        try:
            obj, end = decoder.raw_decode(text[idx:])
            objs.append(obj)
            idx = idx + end
        except JSONDecodeError:
            # try to heuristically extract until matching brace then repair
            start = idx
            depth = 0
            i = idx
            while i < length:
                if text[i] == "{":
                    depth += 1
                elif text[i] == "}":
                    depth -= 1
                    if depth == 0:
                        candidate = text[start:i+1]
                        candidate_fixed = repair_commas(candidate)
                        try:
                            obj = json.loads(candidate_fixed)
                            objs.append(obj)
                            idx = i + 1
                            break
                        except Exception:
                            # give up on this start, move on
                            idx = start + 1
                            break
                i += 1
            else:
                # no matching brace
                idx = start + 1
    return objs

def ensure_dirs():
    os.makedirs(os.path.join(PROJECT_ROOT, "raw_zone"), exist_ok=True)
    os.makedirs(os.path.join(PROJECT_ROOT, "processed_zone"), exist_ok=True)

def main(mock_path="mock_input.txt"):
    ensure_dirs()
    mock_full = os.path.join(PROJECT_ROOT, mock_path)
    if not os.path.exists(mock_full):
        logger.warning(f"Mock file not found: {mock_full} — creating a sample mock_input.txt")
        sample = r"""
# mock_input.txt
# Source: example-scrape
# scraped_at: 2025-11-10T14:22:00+05:30

--- METADATA (key: value lines)
source: https://example.com/product/widget-a
scraper: simple-scraper-v1
lang: en
publisher: Example Corp
contact: support@example.com

--- RAW PARAGRAPH
Widget A is a compact device for everyday use. It comes in multiple colors and often appears in scraped pages with noisy markup and unrelated text like promotional banners, comments, or code snippets. The price information is inconsistent in these pages and sometimes appears as "9.99 USD", "$9.99", or "9,99". Dates observed in different locales: 12/13/2025 and 13/12/2025 (ambiguous). Some text contains OCR errors from PDFs: \"l0cation\" instead of \"location\" and \"O\" vs \"0\" confusion.

--- INLINE JSON (well-formed)
{
  "id": "prod-1001",
  "title": "Widget A",
  "slug": "widget-a",
  "pricing": {
    "price_usd": "9.99",
    "inventory": 120,
    "currency_hint": "USD"
  },
  "tags": ["gadget","home","widget"],
  "dimensions": {"w_mm": 120, "h_mm": 45, "d_mm": 30},
  "release_date": "2025-11-01"
}

--- MALFORMED JSON FRAGMENT (common in scraped text)
{ "id": "prod-1001-b", "title": "Widget B", "specs": { "color": "red", "weight": "0.5kg", }  "notes": "missing comma and trailing comma issues" 

--- HTML SNIPPET (embedded table)
<div class="reviews">
  <h3>Customer Reviews</h3>
  <table>
    <thead><tr><th>author</th><th>rating</th><th>comment</th><th>date</th></tr></thead>
    <tbody>
      <tr><td>Alice</td><td>5</td><td>Excellent product.</td><td>2025-10-20</td></tr>
      <tr><td>Bob</td><td>4</td><td>Good value for money.</td><td>20/10/2025</td></tr>
      <tr><td>Charlie</td><td>3</td><td>Okay, but packaging was bad.</td><td>Oct 19, 2025</td></tr>
    </tbody>
  </table>
</div>

--- CSV-LIKE SECTION
author,rating,helpful_votes,date
Dave,4,2,2025-10-18
Eve,2,0,18-10-2025
Mallory,5,10,2025/10/17

--- KEY-VALUE KVP BLOCK (no fixed structure)
title: Widget A - Special Edition
price: $9.99
currency: USD
availability: In Stock
tags: gadget;home;clearance

--- JSON-LD (schema.org style)
<script type="application/ld+json">
{
  "@context": "http://schema.org/",
  "@type": "Product",
  "name": "Widget A",
  "image": [
    "https://example.com/images/widget-a-1.jpg",
    "https://example.com/images/widget-a-2.jpg"
  ],
  "description": "A versatile widget for the modern home.",
  "sku": "WA-1001",
  "offers": {
    "@type": "Offer",
    "priceCurrency": "USD",
    "price": "9.99",
    "availability": "http://schema.org/InStock",
    "url": "https://example.com/product/widget-a"
  }
}
</script>

--- INLINE CSV TABLE (tabular HTML converted text captured)
ProductID,Name,Color,Stock
prod-1001,Widget A,black,120
prod-1001,Widget A,white,80
prod-1002,Widget B,red,50

--- FREE TEXT WITH NOISE & JS SNIPPET
<!-- Some scraped page contains inline scripts and comments -->
<script>var config = {id: 'prod-1001', price: '9.99', promo: true};</script>
Random footer text - Contact us at (555) 123-4567. Promo code: SAVE10.
Note: DO NOT RUN SQL: \"DROP TABLE users;\" included as sample text from scraped code blocks.

--- OCR-LIKE PAGE FOOTER
Page 3 of 12
Document title: Product catalog - Example Corp
l0cation: Warehouse 9
Total items: one hundred and twenty (120)

--- SQL-LIKE SNIPPET (should be extracted as code, not executed)
-- Example of raw SQL shown on a page, should not be executed
SELECT id, title, price FROM products WHERE price < 20;

--- REPEATED FIELDS (conflicting values across page)
price_usd: 9.99
price: "$9.99"
legacy_price: "9.9900"
currency_hint: USD

--- AMBIGUOUS TYPES / MISSING DATA
views: "1024"
views: "N/A"
sold: "12"
rating_avg: "4.2"
is_featured: "true"
is_limited: 0

--- VARIANT / EVOLUTION BLOCK (v2) - show how source may change over time
=== VARIANT v2 START ===
# New upload on 2025-11-20: product schema changed
{
  "id": "prod-1001",
  "title": "Widget A",
  "slug": "widget-a",
  "pricing": {
    "price": 9.99,
    "currency": "USD"
  },
  "tags": ["gadget","home","widget","sale"],
  "dimensions": {"w_mm": 120, "h_mm": 45, "d_mm": 30},
  "release_date": "2025-11-01",
  "metadata": {
    "imported_from": "example-scrape-v2",
    "ingested_at": "2025-11-20T09:10:00+05:30"
  },
  "views": "N/A",              # type flip: previously numeric, now sometimes N/A
  "ratings": {
    "avg": 4.1,
    "count": 235
  }
}

# Extra CSV row introduced in v2
ProductID,Name,Color,Stock,warehouse
prod-1001,Widget A,black,120,W-9
prod-1001,Widget A,white,80,W-9
prod-1003,Widget C,blue,30,W-12

# New front-matter like YAML (in v2 some pages include it)
---
title: "Widget A - 2025"
published: 2025-11-18
authors:
  - "Editorial Team"
tags:
  - "gadget"
  - "featured"
---

=== VARIANT v2 END ===

--- END OF FILE ---
"""
        try:
            with open(mock_full, "w", encoding="utf-8") as mf:
                mf.write(sample.strip() + "\n")
            logger.info(f"Created sample mock file at: {mock_full}")
        except Exception as e:
            logger.error(f"Failed to create mock file: {e}")
            sys.exit(1)

    text = open(mock_full, "r", encoding="utf-8").read()
    logger.info("Extracting JSON objects from mock input...")
    objs = extract_json_objects(text)
    logger.info(f"Found {len(objs)} JSON object(s)")

    if not objs:
        logger.warning("No JSON objects found — creating a minimal placeholder record")
        objs = [{
            "id": "mock-000",
            "title": "Mock Item",
            "pricing": {"price_usd": "0.0"},
        }]

    timestamp = datetime.utcnow()
    raw_filename = f"mock_raw_{timestamp.strftime('%Y%m%d_%H%M%S')}.json"
    raw_path = os.path.join(PROJECT_ROOT, "raw_zone", raw_filename)
    with open(raw_path, "w", encoding="utf-8") as rf:
        json.dump(objs, rf, indent=2, default=str)
    logger.info(f"Wrote raw file: {raw_path}")

    # call existing classification service
    processed_filename = f"mock_processed_{timestamp.strftime('%Y%m%d_%H%M%S')}.json"
    processed_path = os.path.join(PROJECT_ROOT, "processed_zone", processed_filename)

    try:
        process_data(raw_path, processed_path)
        logger.info(f"Processed file written to: {processed_path}")
    except Exception as e:
        logger.error(f"Processing failed: {e}")
        raise

if __name__ == "__main__":
    main()