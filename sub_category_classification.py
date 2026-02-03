from datetime import datetime, timezone
import json
import os
from typing import List, Dict, Tuple

from openai import OpenAI, APIError, RateLimitError

from category_classification import JsonStore, RetryUtil, CategoryMessagesRepo



def chunked(items: List[str], size: int):
    for i in range(0, len(items), size):
        yield items[i:i + size], i


def safe_json_extract(text: str) -> dict:
    """
    Extract the FIRST valid JSON object from text.
    Raises ValueError if none found.
    """
    start = text.find("{")
    if start == -1:
        raise ValueError("No JSON object found in response")

    for end in range(len(text), start, -1):
        try:
            return json.loads(text[start:end])
        except json.JSONDecodeError:
            continue

    raise ValueError("Failed to parse JSON from model output")


class SubcategoryRegistryRepo:
    def __init__(self, store: JsonStore):
        self.store = store

    def load(self) -> dict:
        return self.store.load({})

    def get_known(self, category: str) -> List[str]:
        data = self.load()
        return data.get(category, {}).get("known_subcategories", [])

    def update(self, category: str, subcategories: List[str]):
        data = self.load()
        data[category] = {
            "known_subcategories": sorted(set(subcategories)),
            "updated_at": datetime.now(timezone.utc).isoformat()
        }
        self.store.save(data)


class SubcategoryMessagesRepo:
    def __init__(self, store: JsonStore):
        self.store = store

    def load(self) -> dict:
        return self.store.load({})

    def append_bulk(self, category: str, subcat_to_msgs: Dict[str, List[dict]]):
        data = self.load()
        data.setdefault(category, {})
        for subcat, msgs in subcat_to_msgs.items():
            data[category].setdefault(subcat, []).extend(msgs)
        self.store.save(data)

class SubcategoryAggregatesRepo:
    def __init__(self, store: JsonStore):
        self.store = store

    def load(self) -> dict:
        return self.store.load({})

    def save(self, data: dict):
        self.store.save(data)


class SubcategoryAggregator:
    @staticmethod
    def apply(
        aggregates: dict,
        category: str,
        contact_id: str,
        counts: Dict[str, int]
    ):
        aggregates.setdefault(category, {})
        for subcat, cnt in counts.items():
            if cnt <= 0:
                continue

            meta = aggregates[category].setdefault(subcat, {
                "message_count": 0,
                "unique_user_ids": []
            })

            meta["message_count"] += cnt
            if contact_id not in meta["unique_user_ids"]:
                meta["unique_user_ids"].append(contact_id)

        return aggregates



class OpenAISubcategoryClassifier:
    def __init__(self, client: OpenAI, model: str = "gpt-4.1-mini"):
        self.client = client
        self.model = model

    def classify(
        self,
        category: str,
        known_subcategories: List[str],
        messages: List[str]
    ) -> Tuple[List[str], List[str]]:
        """
        Returns:
          (all_subcategories, assignments_per_message)
        """

        def call():
            resp = self.client.responses.create(
                model=self.model,
                input=[
                    {
                        "role": "system",
                        "content": (
                            f"You are a deterministic classification engine.\n\n"
                            f"The parent category is: \"{category}\".\n\n"
                            "RULES:\n"
                            "1. EACH message gets EXACTLY ONE sub-category.\n"
                            "2. Reuse existing sub-categories when possible.\n"
                            "3. Create new sub-categories ONLY if necessary.\n"
                            "4. Sub-category names must be short and reusable.\n"
                            "5. Output JSON ONLY.\n\n"
                            "FORMAT:\n"
                            "{\n"
                            "  \"subcategories\": [...],\n"
                            "  \"assignments\": [\n"
                            "    {\"id\": 0, \"subcategory\": \"...\"}\n"
                            "  ]\n"
                            "}"
                        )
                    },
                    {
                        "role": "user",
                        "content": json.dumps({
                            "known_subcategories": known_subcategories,
                            "messages": [
                                {"id": i, "text": t}
                                for i, t in enumerate(messages)
                            ]
                        })
                    }
                ],
                temperature=0
            )

            raw = resp.output_text
            print("Raw: ", raw)
            try:
                parsed = safe_json_extract(raw)
            except Exception:
                print("\n[LLM RAW OUTPUT PREVIEW]")
                print(raw[:1000])
                print("[END PREVIEW]\n")
                raise

            subs = parsed["subcategories"]
            print("Subs: ", subs)
            assignments = parsed["assignments"]
            print("Assignments: ", assignments)

            if len(assignments) != len(messages):
                raise ValueError("Assignment count mismatch")

            sub_by_id = {a["id"]: a["subcategory"] for a in assignments}
            return subs, [sub_by_id[i] for i in range(len(messages))]

        return RetryUtil.run(
            call,
            retries=5,
            delay=2,
            retry_on=(APIError, RateLimitError)
        )


class SubcategoryPipeline:
    def __init__(
        self,
        category_messages_repo: CategoryMessagesRepo,
        subcategory_messages_repo: SubcategoryMessagesRepo,
        subcategory_aggregates_repo: SubcategoryAggregatesRepo,
        registry_repo: SubcategoryRegistryRepo,
        classifier: OpenAISubcategoryClassifier,
    ):
        self.category_messages_repo = category_messages_repo
        self.subcategory_messages_repo = subcategory_messages_repo
        self.subcategory_aggregates_repo = subcategory_aggregates_repo
        self.registry_repo = registry_repo
        self.classifier = classifier

    def run(self):
        category_data = self.category_messages_repo.load()
        aggregates = self.subcategory_aggregates_repo.load()
        print("[INFO] loading category data...")

        for category, messages in category_data.items():
            if not messages:
                continue

            known_subs = self.registry_repo.get_known(category)

            CHUNK_SIZE = 50

            texts = [m["message"] for m in messages]
            print("[INFO] processing texts...")
            all_assignments: List[str] = []
            current_subs = known_subs

            for chunk, offset in chunked(texts, CHUNK_SIZE):
                subs, assigned = self.classifier.classify(
                    category=category,
                    known_subcategories=current_subs,
                    messages=chunk
                )

                current_subs = list(set(current_subs) | set(subs))
                all_assignments.extend(assigned)
            print("[INFO] processing subcategories...")
            # Persist registry
            self.registry_repo.update(category, subs)

            subcat_to_msgs: Dict[str, List[dict]] = {}
            counts: Dict[str, Dict[str, int]] = {}

            for m, sub in zip(messages, all_assignments):
                subcat_to_msgs.setdefault(sub, []).append(m)
                counts.setdefault(m["contact_id"], {})
                counts[m["contact_id"]][sub] = counts[m["contact_id"]].get(sub, 0) + 1

            # Persist messages
            self.subcategory_messages_repo.append_bulk(category, subcat_to_msgs)

            # Update aggregates
            for contact_id, sub_counts in counts.items():
                aggregates = SubcategoryAggregator.apply(
                    aggregates, category, contact_id, sub_counts
                )

            self.subcategory_aggregates_repo.save(aggregates)

        print("[done] subcategory pipeline completed")


def run_subcategory_pipeline():
    client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

    pipeline = SubcategoryPipeline(
        category_messages_repo=CategoryMessagesRepo(JsonStore("data/category_messages.json")),
        subcategory_messages_repo=SubcategoryMessagesRepo(JsonStore("data/subcategory_messages.json")),
        subcategory_aggregates_repo=SubcategoryAggregatesRepo(JsonStore("data/subcategory_aggregates.json")),
        registry_repo=SubcategoryRegistryRepo(JsonStore("data/subcategory_registry.json")),
        classifier=OpenAISubcategoryClassifier(client)
    )

    pipeline.run()

if __name__ == "__main__":
    run_subcategory_pipeline()

