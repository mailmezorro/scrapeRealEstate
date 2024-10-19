import psycopg2

def get_column_names(cur, table_name):
    query = f"""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_name = '{table_name}'
    """
    cur.execute(query)
    columns = [row[0] for row in cur.fetchall()]
    return columns


def check_and_insert_or_update(result, cur, main_table_name, delta_table_name, price_table_name,deltaView_counter_name):
    columns_db = get_column_names(cur, main_table_name)
    columns_db.remove('id')
    select_query = f"SELECT {', '.join(columns_db)} FROM {main_table_name} WHERE id_ad = %s"
    cur.execute(select_query, (result.get('id_ad'),))
    existing_row = cur.fetchone()

    if existing_row:
        print(f"Entry with id_ad {result['id_ad']} found. Checking for differences...")
        differences = {}

        for i, column in enumerate(columns_db):
            if existing_row[i] != result.get(column):
                differences[column] = (existing_row[i], result.get(column))  

        if differences:
            print(f"Difference found: {differences}")
            for key, value in differences.items():
                if key == 'scrape_date' or key == 'creation_date' or key == 'active_flag' or key == 'delta_flag':
                    continue
                    
                if key  == 'price':
                    # Store the old price in the delta table
                    insert_price_query = f"INSERT INTO {price_table_name} (id_ad, price, scrape_date) VALUES (%s, %s, %s)"
                    cur.execute(insert_price_query, (result.get('id_ad'), differences.get('price')[0], differences.get('scrape_date')[0]))
                    
                    # Store the new price in the main table
                    update_price_query = f"UPDATE {main_table_name} SET price = %s WHERE id_ad = %s"
                    cur.execute(update_price_query, (result.get('price'), result.get('id_ad')))
                    
                elif key == 'view_counter':
                    # Store the old view_counter in the delta table
                    insert_view_counter_query = f"INSERT INTO {deltaView_counter_name} (id_ad, view_counter, scrape_date) VALUES (%s, %s, %s)"
                    cur.execute(insert_view_counter_query, (result.get('id_ad'), differences.get('view_counter')[0], differences.get('scrape_date')[0]))
                    
                    # Store the new view_counter in the main table
                    update_view_counter_query = f"UPDATE {main_table_name} SET view_counter = %s, scrape_date = %s WHERE id_ad = %s"
                    cur.execute(update_view_counter_query, (result.get('view_counter'), result.get('scrape_date'), result.get('id_ad')))
                else:
                    # Update main_table
                    update_main_query = f"UPDATE {main_table_name} SET {key} = %s, scrape_date = %s WHERE id_ad = %s"
                    cur.execute(update_main_query, (differences.get(key)[1], result.get('scrape_date'), result.get('id_ad')))

                    insert_delta_query = f"INSERT INTO {delta_table_name} (id_ad, attribute_name, old_value, new_value, scrape_date) VALUES (%s, %s, %s, %s, %s)"
                    cur.execute(insert_delta_query, (result.get('id_ad'), key, differences.get(key)[0], differences.get(key)[1], result.get('scrape_date')))


            excluded_keys = ['view_counter', 'scrape_date', 'active_flag', 'creation_date', 'number_of_ads']
            non_excluded_keys = [key for key in differences if key not in excluded_keys]
            if non_excluded_keys:
                delta_flag_query = f"SELECT delta_flag FROM {main_table_name} WHERE id_ad = %s"
                cur.execute(delta_flag_query, (result.get('id_ad'),))
                delta_flag = cur.fetchone()[0]
                if not delta_flag:
                    update_flag_query = f"UPDATE {main_table_name} SET delta_flag = TRUE WHERE id_ad = %s"
                    cur.execute(update_flag_query, (result.get('id_ad'),))
            else:
                print(f"Only excluded keys found, not updating delta_flag.")
        else:
            print(f"No differences found for id_ad {result['id_ad']}. No update required.")
    else:
        print(f"No entry found with id_ad {result['id_ad']}. Adding new entry to the main table...")

        # Add the new entry to the main table
        insert_query = f"""
            INSERT INTO {main_table_name} (
                {', '.join(columns_db)}
            )
            VALUES ({', '.join(['%s' for _ in columns_db])})
        """
        cur.execute(insert_query, tuple(result.get(column) for column in columns_db))


