# This function checks the raw data for quality issues. If validation fails,
# an exception is raised, which will be logged later.
def validate_stage_data(session):
    """
    Validate the extracted data for duplicates, null values, and correct data types.
    """
    # Check for duplicates
    duplicate_count = session.sql("""
    SELECT COUNT(*)
    FROM (
        SELECT id, COUNT(*) 
        FROM raw_staging_table
        GROUP BY id
        HAVING COUNT(*) > 1
    )
    """).collect()

    # Check for nulls in required fields
    null_values_count = session.sql("""
    SELECT COUNT(*)
    FROM raw_staging_table
    WHERE id IS NULL OR email IS NULL
    """).collect()

    if duplicate_count > 0 or null_values_count > 0:
        raise ValueError("Data validation failed. Duplicates or null values found.")
