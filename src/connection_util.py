# This function establishes the Snowflake connection using Snowpark and
# will be used in the subsequent steps.
from snowflake.snowpark import Session
from config_util import SNOWFLAKE_CONFIG


def get_snowflake_session():
    """
    Establish a connection to Snowflake using Snowpark's Session object.
    """
    connection_params = {
        'account': SNOWFLAKE_CONFIG['account'],
        'user': SNOWFLAKE_CONFIG['user'],
        'password': SNOWFLAKE_CONFIG['password'],
        'warehouse': SNOWFLAKE_CONFIG['warehouse'],
        'database': SNOWFLAKE_CONFIG['database'],
        'schema': SNOWFLAKE_CONFIG['schema'],
        'role': SNOWFLAKE_CONFIG['role']
    }
    return Session.builder.configs(connection_params).create()


def close_snowflake_session(session):
    """
    Close the Snowflake session.
    """
    session.close()
