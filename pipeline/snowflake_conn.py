import os
import snowflake.connector
from dotenv import load_dotenv

load_dotenv()

# pulled these out into a helper after copy-pasting the connect() block
# into bronze, silver, gold separately and having to fix them all when
# i changed the env var names. lesson learned
def get_connection():
    return snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        database=os.environ["SNOWFLAKE_DATABASE"],
        schema=os.environ.get("SNOWFLAKE_SCHEMA", "ECOMMERCE"),
        warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
        role=os.environ.get("SNOWFLAKE_ROLE", ""),
    )
