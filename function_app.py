import logging
import os
import snowflake.connector
import json
import azure.functions as func

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    try:
        # Snowflake connection details
        user = os.getenv('SNOWFLAKE_USER', 'purushoth')
        password = os.getenv('SNOWFLAKE_PASSWORD', 'Purushoth@123')
        account = os.getenv('SNOWFLAKE_ACCOUNT', 'eb83230.west-us-2.azure')
        warehouse = os.getenv('SNOWFLAKE_WAREHOUSE', 'COMPUTE_WH')
        database = os.getenv('SNOWFLAKE_DATABASE', 'SUPPLY_CHAIN')
        schema = os.getenv('SNOWFLAKE_SCHEMA', 'PROSPEX_BY_SRC.PRODUCTS')
        role = os.getenv('SNOWFLAKE_ROLE', 'ACCOUNTADMIN')

        # Connect to Snowflake
        conn = snowflake.connector.connect(
            user=user,
            password=password,
            account=account,
            warehouse=warehouse,
            database=database,
            schema=schema,
            role=role
        )

        cur = conn.cursor()
        cur.execute("SELECT * FROM SUPPLY_CHAIN.PROSPEX_BY_SRC.PRODUCTS LIMIT 10")

        # Fetch results
        rows = cur.fetchall()
        columns = [desc[0] for desc in cur.description]
        results = [dict(zip(columns, row)) for row in rows]

        cur.close()
        conn.close()

        # Return the results as a JSON response
        return func.HttpResponse(
            json.dumps(results),
            mimetype="application/json",
            status_code=200
        )
    except Exception as e:
        logging.error(f"Error: {str(e)}")
        return func.HttpResponse(
            f"Error: {str(e)}",
            status_code=500
        )
