from __future__ import annotations

import os
import json
import time
import math
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple
from datetime import datetime, timezone

import requests
from dateutil.parser import isoparse
from dotenv import load_dotenv
from openai import OpenAI
from openai import APIConnectionError, APITimeoutError, RateLimitError, APIError

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


class RetryUtil:
    @staticmethod
    def run(fn, retries=5, delay=2, retry_on: Tuple[type, ...] = ()):
        """
        Exponential backoff retry.
        If retry_on is provided, only those exceptions are retried.
        """
        for i in range(retries):
            try:
                return fn()
            except Exception as e:
                if retry_on and not isinstance(e, retry_on):
                    raise
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
        try:
            with open(self.path, "r", encoding="utf-8") as f:
                return json.load(f)
        except json.JSONDecodeError:
            print(f"[warning] Corrupted JSON detected: {self.path} — resetting file")
            return default

    def save(self, data):
        tmp = self.path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        os.replace(tmp, self.path)

# =============================
# Repositories
# =============================

class ContactsRepo:
    def __init__(self, store: JsonStore):
        self.store = store

    def load(self) -> Dict[str, dict]:
        return self.store.load({})

    def upsert(self, contacts: List[Contact]) -> Tuple[int, int]:
        """
        Dedup by contact_id. Persist immediately.
        Returns (added_count, total_count)
        """
        data = self.load()
        added = 0
        for c in contacts:
            if c.contact_id not in data:
                data[c.contact_id] = {"email": c.email, "phone": c.phone}
                added += 1
            else:
                # keep latest non-empty values
                if c.email and not data[c.contact_id].get("email"):
                    data[c.contact_id]["email"] = c.email
                if c.phone and not data[c.contact_id].get("phone"):
                    data[c.contact_id]["phone"] = c.phone
        self.store.save(data)
        return added, len(data)


class CursorRepo:
    def __init__(self, store: JsonStore):
        self.store = store

    def load(self) -> Dict[str, str]:
        return self.store.load({})

    def bulk_update(self, updates: Dict[str, str]):
        data = self.load()
        data.update(updates)
        self.store.save(data)


class AggregatesRepo:
    def __init__(self, store: JsonStore):
        self.store = store

    def load(self) -> dict:
        return self.store.load({"categories": {}, "updated_at": None})

    def save(self, data: dict):
        data["updated_at"] = datetime.now(timezone.utc).isoformat()
        self.store.save(data)


class CategoryMessagesRepo:
    """
    Stores messages grouped by category:
    {
      "Pricing Inquiry": [ {contact_id,email,timestamp,message}, ... ],
      ...
    }
    """
    def __init__(self, store: JsonStore):
        self.store = store

    def load(self) -> dict:
        return self.store.load({})

    def append_bulk(self, category_to_messages: Dict[str, List[dict]]):
        data = self.load()
        for cat, msgs in category_to_messages.items():
            data.setdefault(cat, []).extend(msgs)
        self.store.save(data)

# =============================
# Aggregator
# =============================

class CategoryAggregator:
    """
    Aggregates counts + unique users per category in-memory, but we persist per-user immediately.
    """
    @staticmethod
    def apply_user_counts(aggregates: dict, contact_id: str, counts: Dict[str, int]):
        aggregates.setdefault("categories", {})
        for cat, cnt in counts.items():
            if cnt <= 0:
                continue
            meta = aggregates["categories"].setdefault(cat, {
                "message_count": 0,
                "unique_user_count": 0,
                "unique_user_ids": []
            })
            meta["message_count"] += int(cnt)
            if contact_id not in meta["unique_user_ids"]:
                meta["unique_user_ids"].append(contact_id)
                meta["unique_user_count"] = len(meta["unique_user_ids"])
        return aggregates

# =============================
# API Client
# =============================

class GHLClient:
    def __init__(self, base_url: str, api_key: str, timeout=20):
        self.base_url = base_url.rstrip("/")
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

        page_limit = 500  # API max
        page = 1
        search_after = None

        all_contacts: List[Contact] = []
        seen_contact_ids = set()

        while True:
            payload = {
                "page": page,
                "page_limit": page_limit,
            }

            # Only send search_after after first request
            if search_after:
                payload["search_after"] = search_after

            resp = RetryUtil.run(
                lambda: self.session.post(
                    url,
                    json=payload,
                    headers=self._headers(),
                    timeout=self.timeout
                ),
                retries=5,
                delay=2,
                retry_on=(requests.RequestException,)
            )

            data = resp.json()

            if not data.get("success"):
                print(f"[contacts] API failed: {data.get('message')}")
                break

            contacts = data.get("contacts", [])
            total_available = data.get("total", 0)

            if not contacts:
                print("[contacts] no more contacts, stopping")
                break

            # Deduplication + extraction logic (unchanged in spirit)
            for c in contacts:
                cid = c.get("id")
                email = c.get("email")

                if not cid or not email:
                    continue

                if cid in seen_contact_ids:
                    continue

                seen_contact_ids.add(cid)

                all_contacts.append(
                    Contact(
                        contact_id=str(cid),
                        email=email,
                        phone=c.get("phone")
                    )
                )

            print(
                f"[contacts] page={page}, "
                f"batch={len(contacts)}, "
                f"total={len(all_contacts)}/{total_available}"
            )

            # Stop if we already fetched everything
            if len(all_contacts) >= total_available:
                print("[contacts] fetched all available contacts")
                break

            # Cursor MUST come from last contact
            last_contact = contacts[-1]
            search_after = last_contact.get("search_after")

            if not search_after:
                print("[contacts] missing search_after, stopping to avoid infinite loop")
                break

            page += 1
            time.sleep(0.2)

        return all_contacts

    def fetch_messages(self, email: str) -> dict:
        url = f"{self.base_url}/api/v1/ghl/list-messages"
        return RetryUtil.run(
            lambda: self.session.post(
                url,
                json={"email": email},
                headers=self._headers(),
                timeout=self.timeout
            ).json(),
            retries=5,
            delay=2,
            retry_on=(requests.RequestException,)
        )

    def extract_inbound(self, raw: dict) -> List[InboundMessage]:
        msgs = raw.get("messages", [])
        contact_id = raw.get("contact_id")
        email = raw.get("email")

        if not contact_id or not email:
            return []

        out: List[InboundMessage] = []
        for m in msgs:
            if m.get("direction") != "inbound":
                continue
            body = m.get("body")
            date_added = m.get("date_added")
            if not body or not date_added:
                continue

            out.append(InboundMessage(
                contact_id=str(contact_id),
                email=str(email),
                body=str(body),
                timestamp=str(date_added)
            ))

        return out

# =============================
# OpenAI Classifier
# =============================

class OpenAIClassifier:
    """
    Send ALL new messages for a user in ONE call.
    OpenAI returns PER-MESSAGE assignments.
    We parse and enforce correctness ourselves (SDK-compatible).
    """

    def __init__(self, api_key: str, model: str = "gpt-4.1-mini"):
        if not api_key:
            raise ValueError("OPENAI_API_KEY is required")
        self.client = OpenAI(api_key=api_key)
        self.model = model

        self.allowed_categories = [
            "Trial Class",
            "Plans & Pricing",
            "Discounts & Offers",
            "Subscription",
            "Class Management",
            "Curriculum",
            "Account Management",
            "Billing",
            "Referral",
            "Churn Signals",
            "Technical Questions",
            "Terms of Service",
            "Class Questions",
            "General Questions",
            "Other"
        ]

    def classify_assignments(self, messages: List[str]) -> List[str]:
        if not messages:
            return []

        def call():
            resp = self.client.responses.create(
                model=self.model,
                input=[
                    {
                        "role": "system",
                        "content" : (
                "You are a deterministic classification engine, not a chatbot.\n"
                "\n"
                "YOU MUST FOLLOW THESE RULES EXACTLY:\n"
                "1. Every input message MUST receive EXACTLY ONE category.\n"
                "2. The number of output assignments MUST equal the number of input messages.\n"
                "3. You MUST NOT merge, summarize, group, reorder, or skip messages.\n"
                "4. Each assignment MUST reference the same id as the input message.\n"
                "5. Even short, repetitive, or meaningless messages (e.g. 'ok', 'thanks') MUST still receive a category.\n"
                "6. You MUST use ONLY the allowed categories provided.\n"
                "7. Output MUST be valid JSON ONLY — no text, no explanations, no markdown.\n"
                "\n"
                "REQUIRED OUTPUT FORMAT (JSON ONLY):\n"
                "{\n"
                "  \"assignments\": [\n"
                "    { \"id\": 0, \"category\": \"Pricing Inquiry\" }\n"
                "  ]\n"
                "}\n"
                "\n"
                "If the output does not contain exactly one assignment per input message, the output is INVALID.\n"
            )
                    },
                    {
                        "role": "user",
                        "content": json.dumps({
                            "allowed_categories": self.allowed_categories,
                            "messages": [
                                {"id": i, "text": t}
                                for i, t in enumerate(messages)
                            ]
                        })
                    }
                ],
                temperature=0
            )

            # ---- SAFE JSON EXTRACTION ----
            raw = resp.output_text
            start = raw.find("{")
            end = raw.rfind("}")

            if start == -1 or end == -1:
                raise ValueError("OpenAI returned no JSON")

            parsed = json.loads(raw[start:end + 1])
            assignments = parsed.get("assignments")

            if not isinstance(assignments, list):
                raise ValueError("Invalid assignments structure")

            # Enforce one assignment per message
            if len(assignments) != len(messages):
                raise ValueError(
                    f"Assignment length mismatch: expected {len(messages)}, got {len(assignments)}"
                )

            cat_by_id = {}
            for a in assignments:
                if (
                    not isinstance(a, dict)
                    or "id" not in a
                    or "category" not in a
                    or a["category"] not in self.allowed_categories
                ):
                    raise ValueError(f"Invalid assignment: {a}")
                cat_by_id[a["id"]] = a["category"]

            # Guarantee order
            return [cat_by_id[i] for i in range(len(messages))]

        return RetryUtil.run(
            call,
            retries=6,
            delay=2,
            retry_on=(APIConnectionError, APITimeoutError, RateLimitError, APIError)
        )


# =============================
# Pipeline
# =============================

class Pipeline:
    def __init__(
        self,
        client: GHLClient,
        contacts_repo: ContactsRepo,
        cursor_repo: CursorRepo,
        aggregates_repo: AggregatesRepo,
        category_messages_repo: CategoryMessagesRepo,
        classifier: OpenAIClassifier,
    ):
        self.client = client
        self.contacts_repo = contacts_repo
        self.cursor_repo = cursor_repo
        self.aggregates_repo = aggregates_repo
        self.category_messages_repo = category_messages_repo
        self.classifier = classifier

    def run(self):
        # 1) Fetch contacts (paged), store + dedup
        contacts = self.client.fetch_contacts()
        added, total = self.contacts_repo.upsert(contacts)
        print(f"[contacts] upserted added={added} total={total}")

        contacts_map = self.contacts_repo.load()
        cursors = self.cursor_repo.load()

        # Load aggregates once, but persist per user (real-time)
        aggregates = self.aggregates_repo.load()

        for idx, (cid, meta) in enumerate(contacts_map.items(), start=1):
            email = meta.get("email")
            if not email:
                continue

            last_ts = cursors.get(cid)

            # 2) Fetch messages for user
            try:
                raw = self.client.fetch_messages(email)
            except Exception as e:
                print(f"[skip] fetch_messages email={email} error={e}")
                continue

            inbound_all = self.client.extract_inbound(raw)
            if not inbound_all:
                continue

            # 3) Filter only NEW messages (timestamp > cursor), dedup within batch
            inbound_new: List[InboundMessage] = []
            seen_keys = set()

            for m in inbound_all:
                if last_ts and TimeUtil.parse_iso(m.timestamp) <= TimeUtil.parse_iso(last_ts):
                    continue
                key = (m.timestamp, m.body.strip())
                if key in seen_keys:
                    continue
                seen_keys.add(key)
                inbound_new.append(m)

            if not inbound_new:
                continue

            inbound_new.sort(key=lambda x: TimeUtil.parse_iso(x.timestamp))
            texts = [m.body for m in inbound_new]

            # 4) Send ALL new messages for this user to OpenAI in ONE call
            try:
                assigned_categories = self.classifier.classify_assignments(texts)
            except Exception as e:
                # Critical: do NOT update cursor if OpenAI fails
                print(f"[skip] openai_failed email={email} error={e}")
                continue

            # 5) Build counts + category->messages mapping
            counts: Dict[str, int] = {}
            cat_to_msgs: Dict[str, List[dict]] = {}

            for m, cat in zip(inbound_new, assigned_categories):
                counts[cat] = counts.get(cat, 0) + 1
                cat_to_msgs.setdefault(cat, []).append({
                    "contact_id": cid,
                    "email": m.email,
                    "timestamp": m.timestamp,
                    "message": m.body
                })

            # 6) Persist category messages immediately (real-time)
            self.category_messages_repo.append_bulk(cat_to_msgs)

            # 7) Update aggregates immediately (real-time)
            aggregates = CategoryAggregator.apply_user_counts(aggregates, cid, counts)
            self.aggregates_repo.save(aggregates)

            # 8) Update cursor immediately (real-time) — ONLY after success
            newest_ts = inbound_new[-1].timestamp
            self.cursor_repo.bulk_update({cid: newest_ts})
            cursors[cid] = newest_ts  # keep in-memory current

            if idx % 25 == 0:
                print(f"[progress] contacts_processed={idx}")

        print("[done] pipeline completed")

# =============================
# Main
# =============================

def main():
    upcademy_base_url = os.getenv("UPCADEMY_BASE_URL", "").strip()
    upcademy_api_key = os.getenv("UPCADEMY_API_KEY", "").strip()
    openai_api_key = os.getenv("OPENAI_API_KEY", "").strip()

    if not upcademy_base_url or not upcademy_api_key:
        raise ValueError("Missing UPCADEMY_API_KEY or GHL_API_KEY")
    if not openai_api_key:
        raise ValueError("Missing OPENAI_API_KEY")

    # Stores
    contacts_store = JsonStore("data/contacts.json")
    cursors_store = JsonStore("data/cursors.json")
    aggregates_store = JsonStore("data/aggregates.json")
    category_messages_store = JsonStore("data/category_messages.json")

    # Repos
    contacts_repo = ContactsRepo(contacts_store)
    cursor_repo = CursorRepo(cursors_store)
    aggregates_repo = AggregatesRepo(aggregates_store)
    category_messages_repo = CategoryMessagesRepo(category_messages_store)

    # Clients
    upcademy_client = GHLClient(
        base_url=upcademy_base_url,
        api_key=upcademy_api_key,
        timeout=25
    )
    classifier = OpenAIClassifier(api_key=openai_api_key, model=os.getenv("OPENAI_MODEL", "gpt-4.1-mini"))

    pipeline = Pipeline(
        client=upcademy_client,
        contacts_repo=contacts_repo,
        cursor_repo=cursor_repo,
        aggregates_repo=aggregates_repo,
        category_messages_repo=category_messages_repo,
        classifier=classifier
    )
    pipeline.run()


if __name__ == "__main__":
    main()