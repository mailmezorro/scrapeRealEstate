import logging
import psycopg2
from psycopg2 import sql

class PostgresPipeline:
    def __init__(self, postgres_host, postgres_user, postgres_password, postgres_db, postgres_port, main_table_name):
        self.postgres_host = postgres_host
        self.postgres_user = postgres_user
        self.postgres_password = postgres_password
        self.postgres_db = postgres_db
        self.postgres_port = postgres_port
        self.main_table_name = main_table_name
        self.logger = logging.getLogger(__name__)

    @classmethod
    def from_crawler(cls, crawler):
        """Creates the pipeline from Scrapy settings."""
        return cls(
            postgres_host=crawler.settings.get('POSTGRES_HOST'),
            postgres_user=crawler.settings.get('POSTGRES_USER'),
            postgres_password=crawler.settings.get('POSTGRES_PASSWORD'),
            postgres_db=crawler.settings.get('POSTGRES_DB'),
            postgres_port=crawler.settings.get('POSTGRES_PORT', 5432),
            main_table_name=crawler.settings.get('MAIN_TABLE_NAME'),
        )

    def open_spider(self, spider):
        """Opens the connection to the PostgreSQL database."""
        self.logger.info("Connecting to PostgreSQL database...")
        self.conn = psycopg2.connect(
            host=self.postgres_host,
            user=self.postgres_user,
            password=self.postgres_password,
            dbname=self.postgres_db,
            port=self.postgres_port
        )
        self.cursor = self.conn.cursor()
        self.logger.info("Connection to PostgreSQL successful.")

    def close_spider(self, spider):
        """Closes the connection to the PostgreSQL database."""
        self.logger.info("Closing PostgreSQL connection.")
        self.cursor.close()
        self.conn.close()

    def process_item(self, item, spider):
        """Inserts or updates an item in the main table."""
        try:
            self.insert_or_update_main_table(item)

            self.conn.commit()
            self.logger.info(f"Item processed successfully: {item['id_ad']}")
        except Exception as e:
            self.logger.error(f"Error processing item {item['id_ad']}: {e}", exc_info=True)
            self.conn.rollback()
        return item

    def insert_or_update_main_table(self, item):
        """Inserts an item or updates it in the main table."""
        try:
            # Columns and placeholders for the SQL query
            columns = item.keys()
            placeholders = [f"%({col})s" for col in columns]
            updates = [f"{col} = EXCLUDED.{col}" for col in columns if col != "id_ad"]

            query = sql.SQL("""
                INSERT INTO {table} ({columns})
                VALUES ({values})
                ON CONFLICT (id_ad) DO UPDATE SET {updates}
                RETURNING id_ad;
            """).format(
                table=sql.Identifier(self.main_table_name),
                columns=sql.SQL(", ").join(map(sql.Identifier, columns)),
                values=sql.SQL(", ").join(map(sql.SQL, placeholders)),
                updates=sql.SQL(", ").join(map(sql.SQL, updates))
            )

            self.cursor.execute(query, dict(item))
            result = self.cursor.fetchone()

            if result:
                self.logger.info(f"Main table successfully updated or inserted for id_ad: {result[0]}")
            else:
                self.logger.warning(f"No changes made for id_ad: {item['id_ad']}")
        except Exception as e:
            self.logger.error(f"Error during insert/update for id_ad: {item['id_ad']}: {e}", exc_info=True)
