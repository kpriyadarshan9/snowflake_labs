# This function uses the established session to execute the SQL command that extracts
# data from the internal stage into a staging table.
def extract_data_from_stage(session):
    """
    Extract data from the internal Snowflake stage using the Snowpark session.
    """
    query = """
    COPY INTO raw_staging_table
    FROM @my_internal_stage
    FILE_FORMAT = (TYPE = 'CSV', SKIP_HEADER = 1, FIELD_OPTIONALLY_ENCLOSED_BY = '"');
    """
    session.sql(query).collect()
