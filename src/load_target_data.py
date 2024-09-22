# This function moves validated data from the staging table to the target table
# for further analytics or reporting.
def load_data_into_target(session):
    """
    Load the cleaned and validated data into the final target table.
    """
    query = """
    INSERT INTO final_table (id, name, email, signup_date, amount_spent)
    SELECT id, name, email, signup_date, amount_spent
    FROM raw_staging_table
    WHERE id IS NOT NULL AND email IS NOT NULL;
    """
    session.sql(query).collect()
