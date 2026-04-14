"""
PostgreSQL database connector for storing processed data.
Handles all operations related to database connections and data insertion.
"""
import logging
from typing import List, Optional, Dict, Any

from sqlalchemy import create_engine, inspect, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError
import pandas as pd

from src.shared.config.settings import settings
from src.domain.entities.weather import WeatherRecord


logger = logging.getLogger(__name__)


class PostgreSQLConnector:
    """
    Connector for PostgreSQL database.
    Handles database connections, table creation, and data insertion.
    """

    def __init__(self, connection_string: Optional[str] = None):
        """
        Initialize PostgreSQL connector.

        Args:
            connection_string: SQLAlchemy connection string
        """
        self.connection_string = connection_string or settings.POSTGRES_SQLALCHEMY_CONN
        self.engine: Optional[Engine] = None
        self._connect()

    def _connect(self) -> None:
        """Establish database connection."""
        try:
            self.engine = create_engine(self.connection_string, echo=False)
            # Test connection
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            logger.info("Successfully connected to PostgreSQL database")
        except SQLAlchemyError as e:
            logger.error(f"Failed to connect to PostgreSQL: {e}")
            raise

    def disconnect(self) -> None:
        """Close database connection."""
        if self.engine:
            self.engine.dispose()
            logger.info("Disconnected from PostgreSQL database")

    def table_exists(self, table_name: str) -> bool:
        """
        Check if table exists in database.

        Args:
            table_name: Name of the table

        Returns:
            True if table exists, False otherwise
        """
        try:
            inspector = inspect(self.engine)
            return table_name in inspector.get_table_names()
        except SQLAlchemyError as e:
            logger.error(f"Error checking table existence: {e}")
            return False

    def create_weather_data_table(self) -> bool:
        """
        Create the weather_data table if it doesn't exist.
        This is for the raw staging layer.

        Returns:
            True if table exists or was created successfully
        """
        if self.table_exists('weather_data'):
            logger.info("Table 'weather_data' already exists")
            return True

        create_table_sql = """
        CREATE TABLE IF NOT EXISTS weather_data (
            id SERIAL PRIMARY KEY,
            city VARCHAR(100) NOT NULL,
            timestamp TIMESTAMP NOT NULL,
            temperature FLOAT NOT NULL,
            humidity INTEGER NOT NULL,
            wind_speed FLOAT NOT NULL,
            description VARCHAR(255),
            pressure INTEGER,
            clouds INTEGER,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        
        CREATE INDEX IF NOT EXISTS idx_weather_city_timestamp 
        ON weather_data(city, timestamp DESC);
        CREATE INDEX IF NOT EXISTS idx_weather_timestamp 
        ON weather_data(timestamp DESC);
        """

        try:
            with self.engine.connect() as conn:
                conn.execute(text(create_table_sql))
                conn.commit()
            logger.info("Successfully created 'weather_data' table")
            return True
        except SQLAlchemyError as e:
            logger.error(f"Failed to create weather_data table: {e}")
            return False

    def create_dimension_tables(self) -> bool:
        """
        Create dimension and fact tables for the data warehouse (gold layer).

        Returns:
            True if tables were created successfully
        """
        create_tables_sql = """
        -- Dimension: City
        CREATE TABLE IF NOT EXISTS dim_city (
            city_id SERIAL PRIMARY KEY,
            city_name VARCHAR(100) NOT NULL UNIQUE,
            country VARCHAR(100) NOT NULL,
            latitude FLOAT,
            longitude FLOAT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        -- Fact: Weather
        CREATE TABLE IF NOT EXISTS fact_weather (
            weather_id SERIAL PRIMARY KEY,
            city_id INTEGER NOT NULL REFERENCES dim_city(city_id),
            measurement_timestamp TIMESTAMP NOT NULL,
            temperature FLOAT NOT NULL,
            humidity INTEGER NOT NULL,
            wind_speed FLOAT NOT NULL,
            description VARCHAR(255),
            pressure INTEGER,
            clouds INTEGER,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        -- Create indexes for better query performance
        CREATE INDEX IF NOT EXISTS idx_fact_weather_city_timestamp 
        ON fact_weather(city_id, measurement_timestamp DESC);
        CREATE INDEX IF NOT EXISTS idx_fact_weather_timestamp 
        ON fact_weather(measurement_timestamp DESC);
        CREATE INDEX IF NOT EXISTS idx_dim_city_name 
        ON dim_city(city_name);
        """

        try:
            with self.engine.connect() as conn:
                conn.execute(text(create_tables_sql))
                conn.commit()
            logger.info("Successfully created dimension and fact tables")
            return True
        except SQLAlchemyError as e:
            logger.error(f"Failed to create dimension/fact tables: {e}")
            return False

    def insert_dataframe(self,
                        df: pd.DataFrame,
                        table_name: str,
                        if_exists: str = 'append') -> bool:
        """
        Insert DataFrame into table.

        Args:
            df: pandas DataFrame to insert
            table_name: Target table name
            if_exists: How to behave if table exists ('append', 'replace', 'fail')

        Returns:
            True if insertion successful, False otherwise
        """
        try:
            df.to_sql(table_name, self.engine, if_exists=if_exists, index=False)
            logger.info(f"Successfully inserted {len(df)} rows into '{table_name}'")
            return True
        except SQLAlchemyError as e:
            logger.error(f"Failed to insert data into PostgreSQL: {e}")
            return False

    def execute_query(self, query: str) -> Optional[List[Dict[str, Any]]]:
        """
        Execute a SELECT query and return results.

        Args:
            query: SQL query to execute

        Returns:
            List of result rows as dictionaries, or None if error
        """
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text(query))
                rows = result.fetchall()
                return [dict(row._mapping) for row in rows]
        except SQLAlchemyError as e:
            logger.error(f"Failed to execute query: {e}")
            return None

    def execute_update(self, query: str) -> bool:
        """
        Execute an UPDATE/INSERT/DELETE query.

        Args:
            query: SQL query to execute

        Returns:
            True if execution successful, False otherwise
        """
        try:
            with self.engine.connect() as conn:
                conn.execute(text(query))
                conn.commit()
            logger.info("Query executed successfully")
            return True
        except SQLAlchemyError as e:
            logger.error(f"Failed to execute update query: {e}")
            return False

    def get_latest_timestamp(self, table_name: str) -> Optional[str]:
        """
        Get the latest timestamp from a table.

        Args:
            table_name: Table name

        Returns:
            Latest timestamp as string, or None if error
        """
        query = f"SELECT MAX(timestamp) as latest FROM {table_name};"
        results = self.execute_query(query)

        if results and results[0].get('latest'):
            return str(results[0]['latest'])
        return None
