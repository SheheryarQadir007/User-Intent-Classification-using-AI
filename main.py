import os

from intent import Pipeline, GHLClient, ContactsRepo, CursorRepo, AggregatesRepo, OpenAIClassifier, JsonStore

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
        # salt=os.getenv("USER_HASH_SALT")
    )
    pipeline.run()

if __name__ == "__main__":
    main()
