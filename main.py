import os

from intent import (
    Pipeline,
    GHLClient,
    ContactsRepo,
    CursorRepo,
    AggregatesRepo,
    OpenAIClassifier,
    JsonStore,
    CategoryMessagesRepo
)

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
        messages_repo=CategoryMessagesRepo(JsonStore(os.path.join("data", "messages_by_category.json")))
    )
    pipeline.run()

if __name__ == "__main__":
    main()
