import datetime

def convert_to_int(result, keys_to_convert):
    for key in keys_to_convert:
        try:
            result[key] = int(result[key])
        except (ValueError, TypeError, KeyError):
            result[key] = None
    return result
    
def convert_to_float(result, keys_to_convert):
    for key in keys_to_convert:
        try:
            result[key] = float(result[key])
        except (ValueError, TypeError, KeyError):
            result[key] = None
    return result
    
def convert_to_date(values):
    for value in values:
        try:
            return datetime.strptime(value, "%d.%m.%Y")
        except ValueError:
            return None
        
        
def rename_key(dictionary, old_key, new_key):
    if old_key in dictionary:
        dictionary[new_key] = dictionary.pop(old_key)
    return dictionary

    