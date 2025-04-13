import json

with open("test_output.json", "r", encoding="utf-8") as f:
    data = json.load(f)

assert len(data) > 0, "No data scraped."

for i, item in enumerate(data):
    assert item.get("title"), f"Missing title in item {i}"
    assert item.get("price") is not None, f"Missing price in item {i}"
    assert item.get("location"), f"Missing location in item {i}"
    assert item.get("description") is not None, f"Missing description in item {i}"
    assert item.get("creation_date") is not None, f"Missing creation_date in item {i}"
    assert item.get("scrape_date") is not None, f"Missing scrape_date in item {i}"

    assert item.get("id_ad"), f"Missing id_ad in item {i}"
    assert item.get("link"), f"Missing link in item {i}"

    # Optional numeric fields – if present, they must be float/int
    for field in ["bedrooms", "living_area", "plot_area", "rooms", "bathrooms", "floors", "year_built", "view_counter", "number_of_ads"]:
        value = item.get(field)
        if value is not None:
            assert isinstance(value, (int, float)), f"{field} is not a number in item {i}: {value}"

    # Optional strings – if present, they must be strings
    for field in ["commission", "house_type", "seller_name", "user_type", "active_since"]:
        value = item.get(field)
        if value is not None:
            assert isinstance(value, str), f"{field} is not a string in item {i}: {value}"

    # Coordinates – if set, check if they are floats
    for field in ["latitude", "longitude"]:
        value = item.get(field)
        if value is not None:
            try:
                float(value)
            except ValueError:
                assert False, f"{field} is not a valid float in item {i}: {value}"

    # active_flag should always be True or False
    assert isinstance(item.get("active_flag"), bool), f"active_flag is not boolean in item {i}"

print("Content smoke test passed.")
