from __future__ import annotations

import os
import json
import time
import math
import hashlib
from dataclasses import dataclass
from typing import Dict, List, Optional, Set
from datetime import datetime, timezone

import requests
from dateutil.parser import isoparse
from dotenv import load_dotenv
from openai import OpenAI

load_dotenv()

# =============================
# Utilities
# =============================

class TimeUtil:
    @staticmethod
    def parse_iso(ts: str):
        dt = isoparse(ts)
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)

    @staticmethod
    def newer(a: Optional[str], b: Optional[str]) -> Optional[str]:
        if not a:
            return b
        if not b:
            return a
        return a if TimeUtil.parse_iso(a) >= TimeUtil.parse_iso(b) else b


class HashUtil:
    @staticmethod
    def user_hash(contact_id: str, salt: str) -> str:
        return hashlib.sha256(f"{salt}:{contact_id}".encode()).hexdigest()

    @staticmethod
    def msg_fp(contact_id: str, ts: str, body: str) -> str:
        return hashlib.sha256(f"{contact_id}|{ts}|{body}".encode()).hexdigest()


class RetryUtil:
    @staticmethod
    def run(fn, retries=3, delay=2):
        for i in range(retries):
            try:
                return fn()
            except Exception as e:
                if i == retries - 1:
                    raise
                sleep = delay * (2 ** i)
                print(f"[retry] attempt={i+1} wait={sleep}s error={e}")
                time.sleep(sleep)

# =============================
# Models
# =============================

@dataclass(frozen=True)
class Contact:
    contact_id: str
    email: str
    phone: Optional[str]


@dataclass(frozen=True)
class InboundMessage:
    contact_id: str
    email: str
    body: str
    timestamp: str

# =============================
# JSON Store
# =============================

class JsonStore:
    def __init__(self, path: str):
        self.path = path
        os.makedirs(os.path.dirname(path), exist_ok=True)

    def load(self, default):
        if not os.path.exists(self.path):
            return default
        with open(self.path, "r") as f:
            return json.load(f)

    def save(self, data):
        tmp = self.path + ".tmp"
        with open(tmp, "w") as f:
            json.dump(data, f, indent=2)
        os.replace(tmp, self.path)

# =============================
# Repositories
# =============================

class ContactsRepo:
    def __init__(self, store: JsonStore):
        self.store = store

    def load(self):
        return self.store.load({})

    def upsert(self, contacts: List[Contact]):
        data = self.load()
        added = 0
        for c in contacts:
            if c.contact_id not in data:
                data[c.contact_id] = {"email": c.email, "phone": c.phone}
                added += 1
        self.store.save(data)
        return added, len(data)


class CursorRepo:
    def __init__(self, store: JsonStore):
        self.store = store

    def load(self):
        return self.store.load({})

    def bulk_update(self, updates: Dict[str, str]):
        data = self.load()
        data.update(updates)
        self.store.save(data)


class AggregatesRepo:
    def __init__(self, store: JsonStore):
        self.store = store

    def load(self):
        return self.store.load({"categories": {}})

    def save(self, data):
        data["updated_at"] = datetime.now(timezone.utc).isoformat()
        self.store.save(data)

# =============================
# Aggregator
# =============================

class CategoryAggregator:
    def __init__(self, salt: str):
        self.salt = salt
        self.counts = {}
        self.users = {}

    def merge(self, persisted):
        for cat, meta in persisted["categories"].items():
            self.counts[cat] = self.counts.get(cat, 0) + meta["message_count"]
            self.users.setdefault(cat, set()).update(meta["unique_user_hashes"])

    def ingest(self, contact_id: str, categories: List[str]):
        uh = HashUtil.user_hash(contact_id, self.salt)
        for cat in categories:
            self.counts[cat] = self.counts.get(cat, 0) + 1
            self.users.setdefault(cat, set()).add(uh)

    def export(self):
        return {
            "categories": {
                cat: {
                    "message_count": self.counts[cat],
                    "unique_user_count": len(self.users[cat]),
                    "unique_user_hashes": list(self.users[cat]),
                }
                for cat in self.counts
            }
        }

# =============================
# API Client (FIXED PAGINATION)
# =============================

class GHLClient:
    def __init__(self, base_url, api_key, timeout=20):
        self.base_url = base_url
        self.api_key = api_key
        self.timeout = timeout
        self.session = requests.Session()

    def _headers(self):
        return {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        }

    def fetch_contacts(self) -> List[Contact]:
        url = f"{self.base_url}/api/v1/ghl/search-contacts"
        page, limit = 1, 100
        all_contacts = []

        # First call to get total
        resp = RetryUtil.run(
            lambda: self.session.post(url, json={"page": 1, "page_limit": limit}, headers=self._headers(), timeout=self.timeout)
        )
        data = resp.json()
        total = data.get("total", 0)
        total_pages = math.ceil(total / limit)

        print(f"[contacts] total={total} pages={total_pages}")

        while page <= total_pages:
            payload = {"page": page, "page_limit": limit}
            resp = RetryUtil.run(
                lambda: self.session.post(url, json=payload, headers=self._headers(), timeout=self.timeout)
            )
            data = resp.json()
            contacts = data.get("contacts", [])

            for c in contacts:
                if c.get("id") and c.get("email"):
                    all_contacts.append(Contact(
                        contact_id=str(c["id"]),
                        email=c["email"],
                        phone=c.get("phone")
                    ))

            print(f"[contacts] fetched page {page}")
            page += 1
            time.sleep(0.2)

        return all_contacts

    def fetch_messages(self, email: str):
        url = f"{self.base_url}/api/v1/ghl/list-messages"
        return RetryUtil.run(
            lambda: self.session.post(url, json={"email": email}, headers=self._headers(), timeout=self.timeout).json()
        )

    def extract_inbound(self, raw: dict) -> List[InboundMessage]:
        msgs = raw.get("messages", [])
        contact_id = raw.get("contact_id")  # ✅ from envelope
        email = raw.get("email")

        if not contact_id or not email:
            return []

        out = []
        for m in msgs:
            if m.get("direction") != "inbound":
                continue

            body = m.get("body")
            date_added = m.get("date_added")

            if not body or not date_added:
                continue

            out.append(InboundMessage(
                contact_id=str(contact_id),
                email=email,
                body=body,
                timestamp=date_added
            ))

        return out

# =============================
# OpenAI Classifier
# =============================

class OpenAIClassifier:
    def __init__(self, api_key: str, model: str = "gpt-4o-mini"):
        if not api_key:
            raise ValueError("OPENAI_API_KEY is required for OpenAIClassifier")
        self.client = OpenAI(api_key=api_key)
        self.model = model

        # Fixed category set (DO NOT change without versioning)
        self.allowed_categories = [
            "Pricing Inquiry",
            "Schedule Inquiry",
            "Trial Booking",
            "Technical Issue",
            "General Question",
            "Other"
        ]

    def classify(self, texts: List[str]) -> List[List[str]]:
        """
        Input:  list of message texts
        Output: list of category lists (same order)
        """

        if not texts:
            return []

        system_prompt = (
            "You are an AI classifier for an education platform.\n"
            "Your job is to categorize user chat messages.\n\n"
            "Rules:\n"
            "- You MUST use ONLY the allowed categories.\n"
            "- Multiple categories per message are allowed.\n"
            "- Do NOT invent new categories.\n"
            "- Do NOT include explanations.\n"
            "- Do NOT repeat the message text.\n"
            "- Output MUST be valid JSON only.\n"
        )

        user_prompt = {
            "allowed_categories": self.allowed_categories,
            "messages": [
                {"id": i, "text": text}
                for i, text in enumerate(texts)
            ],
            "output_format": {
                "results": [
                    ["Category A", "Category B"]
                ]
            }
        }

        response = self.client.chat.completions.create(
            model=self.model,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": json.dumps(user_prompt)}
            ],
            temperature=0
        )

        raw = response.choices[0].message.content.strip()

        try:
            parsed = json.loads(raw)
            results = parsed.get("results")

            # Validate output shape
            if not isinstance(results, list) or len(results) != len(texts):
                raise ValueError("Invalid result length")

            # Validate categories
            for cats in results:
                if not isinstance(cats, list):
                    raise ValueError("Categories must be list")
                for c in cats:
                    if c not in self.allowed_categories:
                        raise ValueError(f"Invalid category: {c}")

            return results

        except Exception as e:
            print(f"[openai] invalid response, falling back. error={e}")
            # Safe fallback: mark all as Other
            return [["Other"] for _ in texts]

# =============================
# Pipeline
# =============================

class Pipeline:
    def __init__(self, client, contacts, cursors, aggregates, classifier, salt):
        self.client = client
        self.contacts = contacts
        self.cursors = cursors
        self.aggregates = aggregates
        self.classifier = classifier
        self.salt = salt

    def run(self):
        contacts = self.client.fetch_contacts()
        self.contacts.upsert(contacts)

        agg = CategoryAggregator(self.salt)
        agg.merge(self.aggregates.load())
        cursors = self.cursors.load()
        cursor_updates = {}
        seen = set()

        for i, cid in enumerate(self.contacts.load()):
            email = self.contacts.load()[cid]["email"]
            last_ts = cursors.get(cid)

            try:
                raw = self.client.fetch_messages(email)
            except Exception as e:
                print(f"[skip] {email} error={e}")
                continue

            for m in self.client.extract_inbound(raw):
                if last_ts and TimeUtil.parse_iso(m.timestamp) <= TimeUtil.parse_iso(last_ts):
                    continue

                fp = HashUtil.msg_fp(cid, m.timestamp, m.body)
                if fp in seen:
                    continue
                seen.add(fp)

                cats = self.classifier.classify([m.body])[0]
                agg.ingest(cid, cats)
                cursor_updates[cid] = TimeUtil.newer(cursor_updates.get(cid), m.timestamp)

            if len(cursor_updates) >= 20:
                self.cursors.bulk_update(cursor_updates)
                cursor_updates.clear()

            if i % 25 == 0 and i > 0:
                print(f"[progress] contacts={i}")

        if cursor_updates:
            self.cursors.bulk_update(cursor_updates)

        self.aggregates.save(agg.export())
        print("[done] pipeline completed")

# =============================
# Main
# =============================

def main():
    pipeline = Pipeline(
        client=GHLClient(os.getenv("UPCADEMY_BASE_URL"), os.getenv("UPCADEMY_API_KEY")),
        contacts=ContactsRepo(JsonStore("data/contacts.json")),
        cursors=CursorRepo(JsonStore("data/cursors.json")),
        aggregates=AggregatesRepo(JsonStore("data/aggregates.json")),
        classifier=OpenAIClassifier(os.getenv("OPENAI_API_KEY")),
        salt=os.getenv("USER_HASH_SALT")
    )
    pipeline.run()

if __name__ == "__main__":
    main()
