import json
import sys

with open("test_output.json", "r", encoding="utf-8") as f:
    data = json.load(f)

assert len(data) > 0, "No data scraped."

for i, item in enumerate(data):
    assert item.get("title"), f"Missing title in item {i}"
    assert item.get("price") is not None, f"Missing price in item {i}"
    assert item.get("location"), f"Missing location in item {i}"
    assert item.get("id_ad"), f"Missing ad ID in item {i}"

print("Content smoke test passed.")
