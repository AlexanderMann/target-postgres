from singer import utils
import psycopg2

from target_postgres.postgres import PostgresTarget
from target_postgres import target_tools

REQUIRED_CONFIG_KEYS = [
    'postgres_database'
]


def cli():
    config = utils.parse_args(REQUIRED_CONFIG_KEYS).config

    with psycopg2.connect(
            host=config.get('postgres_host', 'localhost'),
            port=config.get('postgres_port', 5432),
            dbname=config.get('postgres_database'),
            user=config.get('postgres_username'),
            password=config.get('postgres_password')
    ) as connection:
        target_tools.main(PostgresTarget(
            connection,
            postgres_schema=config.get('postgres_schema', 'public')))
