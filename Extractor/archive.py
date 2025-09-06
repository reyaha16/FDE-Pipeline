import logging
from string import Template
import os
import yaml
from dotenv import load_dotenv
from Extractor.database_connector import DatabaseConnector

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

CONFIG_FILE_PATH = os.path.join(os.path.dirname(__file__), "config.yaml")
load_dotenv()

def load_config():
    with open(CONFIG_FILE_PATH, "r") as file:
        config_content = file.read()
        template = Template(config_content)
        config_content = template.safe_substitute(os.environ)
        return yaml.safe_load(config_content)

def execute_query(conn, query, params=None):
    try:
        with conn.cursor() as cur:
            cur.execute(query, params)
            if cur.description is not None:
                return cur.fetchall()
            conn.commit()
    except Exception as e:
        conn.rollback()
        raise RuntimeError(f"Database error: {str(e)}")

def get_table_columns(conn, table_name, schema):
    table_name = table_name.lower()
    query = f"""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = '{schema}'
        AND table_name = '{table_name}'
        ORDER BY ordinal_position
    """
    rows = execute_query(conn, query)
    return [row[0] for row in rows]

def archive_table(conn, archive_table, source_table):
    archive_schema = "archive"
    source_schema = "landing"

    source_columns = get_table_columns(conn, source_table, source_schema)

    insert_cols = ", ".join(source_columns + ["archived_at"])
    select_cols = ", ".join(source_columns) + ", CURRENT_TIMESTAMP"

    query = f"""
        INSERT INTO {archive_schema}.{archive_table} ({insert_cols})
        SELECT {select_cols} FROM {source_schema}.{source_table}
    """

    execute_query(conn, query)

def archive_data():
    database_connector = DatabaseConnector(load_config())
    conn = database_connector.get_connection()
    config = load_config()

    landing_tables_from_s3 = config["s3"]["files"].values()
    landing_tables_from_api = config["api"]["endpoints"].values()
    landing_tables = list(landing_tables_from_s3) + list(landing_tables_from_api)

    for table in landing_tables:
        archive_table_name = f"archive_{table.lower()}"
        try:
            archive_table(conn, archive_table_name, table)
            logger.info(f"Archived {table}")
        except Exception as e:
            logger.error(f"Failed to archive {table}: {str(e)}")
    conn.close()
    pass 

# Optional: keep main for standalone testing
if __name__ == "__main__":
    archive_data()
