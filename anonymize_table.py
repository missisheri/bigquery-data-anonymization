import os
import datetime
import pandas as pd

from google.cloud import bigquery
from google.api_core.exceptions import NotFound


table_id = "project.dataset.table"

client = bigquery.Client()

def get_pii_columns_list(table_id, client):
    pii_columns = []
    # fetch fields with policy tags in the table using a function get_policy_tags. 
    # This function needs to be created and assumes that policy tags are applied to the piis columns in bigquery.
    piis = get_policy_tags(table_id, client)
    for pii_row in piis:
        pii_columns.append(pii_row["column"])
    return pii_columns


def anonymize_pii_data(table_id):
    # get list of columns with policy tags
    pii_columns = get_pii_columns_list(table_id, client)

    # Fetch the list of IDs already processed for this table_id in the audit_table
    try:
        already_processed_query = f"""
        SELECT DISTINCT id
        FROM `project.dataset.audit_table`
        WHERE table_id = '{table_id}'
        """
        already_processed_job = client.query(already_processed_query)
        already_processed_ids = [row['id'] for row in already_processed_job]
    except NotFound as e:
        print(f"Table audit_table does not exist yet. Continue.")
        already_processed_ids = []
    print(f"audit_table table successfully checked for already processed queries!")
   

    audit_data = []

    # Create temporary table with distinct anonymized data 
    temp_table_id = f"{table_id}_temp"
    create_temp_table_query = f"""CREATE TABLE {temp_table_id} AS   
        WITH first_anonymized_at_row AS (
            SELECT id, MIN(loaded_at) as first_anonymized_at
            FROM `{table_id}`
            WHERE email LIKE '%anonymized%'
            GROUP BY id
        )
        SELECT distinct p.id, {', '.join([f'p.`{column}`' for column in pii_columns])}, f.first_anonymized_at
        FROM `{table_id}` p
        INNER JOIN first_anonymized_at_row f
        ON p.id = f.id AND p.loaded_at = f.first_anonymized_at
        WHERE p.email LIKE '%anonymized%'"""
    temp_job = client.query(create_temp_table_query)
    temp_job.result()
    print(f"{temp_table_id} successfully created!")

    # Merge temporary table with main table
    merge_query = f"""
    MERGE {table_id} AS main
    USING {temp_table_id} AS temp
    ON main.id = temp.id
    WHEN MATCHED THEN
    UPDATE SET {', '.join([f'main.{column} = temp.{column}' for column in pii_columns])}
    """
    merge_job = client.query(merge_query)
    merge_job.result()
    print(f"{temp_table_id} successfully merged with {table_id}!")


    # Record the timestamp after successful MERGE operation
    anonymized_at = datetime.datetime.utcnow()

    # Fetch anonymized data to create audit data
    get_temp_query = f"""SELECT * FROM {temp_table_id}"""
    get_temp_job = client.query(get_temp_query)
    get_temp_job.result()

    # Create audit record from the data that was just merged.
    for row in get_temp_job:
        if row['id'] not in already_processed_ids:
            # get values of fields with piis
            anonymized_values = {column_name: row[column_name] for column_name in pii_columns}
            # get common identifier
            anonymized_values["id"] = row.id
            anonymized_values["first_anonymized_at"] = row.first_anonymized_at
            audit_data.append(anonymized_values)
            print("Audit row created for id: ", anonymized_values["id"])

    if audit_data:
        # Convert audit data to pandas dataframe
        # load to BQ table
        df = pd.json_normalize(audit_data)
        df.insert(0, 'table_id', table_id)
        df["anonymized_at"] = anonymized_at
        dest_dataset="dataset"
        dest_table="audit_table"
        project = "project"
        write_disposition = "WRITE_APPEND"
        schema_update_options = "ALLOW_FIELD_ADDITION"

        audit_table_id = f"{project}.{dest_dataset}.{dest_table}"

        job_config = bigquery.LoadJobConfig(
            write_disposition=write_disposition,
            schema_update_options=schema_update_options,
            autodetect=True,
        )
        job = client.load_table_from_dataframe(
            df, audit_table_id, project=project, job_config=job_config
        )
        status = job.result()
        print("Audit data loaded to audit_table.")
    else:
        print("No new audit data to load.")

    # Delete temporary table
    delete_temp_query = f"""DROP TABLE {temp_table_id}"""
    delete_temp_job = client.query(delete_temp_query)
    delete_temp_job.result()
    print(f"{temp_table_id} successfully deleted!")

anonymize_pii_data(table_id)
