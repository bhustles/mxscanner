"""
Email Processing System - Database Module
Handles PostgreSQL connections and operations
"""

import psycopg2
from psycopg2 import pool, sql, extras
from contextlib import contextmanager
import logging
from typing import List, Dict, Any, Optional, Generator
import io
import csv

from config import DATABASE, DB_CONNECTION_STRING, BATCH_SIZE_LOAD

logger = logging.getLogger(__name__)

# =============================================================================
# CONNECTION POOL
# =============================================================================

_connection_pool: Optional[pool.ThreadedConnectionPool] = None


def init_connection_pool(min_connections: int = 2, max_connections: int = 10) -> None:
    """Initialize the database connection pool."""
    global _connection_pool
    
    if _connection_pool is not None:
        return
    
    try:
        _connection_pool = pool.ThreadedConnectionPool(
            min_connections,
            max_connections,
            **DATABASE
        )
        logger.info(f"Database connection pool initialized (min={min_connections}, max={max_connections})")
    except Exception as e:
        logger.error(f"Failed to initialize connection pool: {e}")
        raise


def close_connection_pool() -> None:
    """Close all connections in the pool."""
    global _connection_pool
    
    if _connection_pool is not None:
        _connection_pool.closeall()
        _connection_pool = None
        logger.info("Database connection pool closed")


@contextmanager
def get_connection() -> Generator[psycopg2.extensions.connection, None, None]:
    """Get a connection from the pool."""
    global _connection_pool
    
    if _connection_pool is None:
        init_connection_pool()
    
    conn = _connection_pool.getconn()
    try:
        yield conn
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise
    finally:
        _connection_pool.putconn(conn)


@contextmanager
def get_cursor() -> Generator[psycopg2.extensions.cursor, None, None]:
    """Get a cursor with automatic connection handling."""
    with get_connection() as conn:
        cursor = conn.cursor()
        try:
            yield cursor
        finally:
            cursor.close()


# =============================================================================
# DATABASE SETUP
# =============================================================================

def create_database() -> bool:
    """Create the email_master database if it doesn't exist."""
    try:
        # Connect to default postgres database
        conn = psycopg2.connect(
            host=DATABASE['host'],
            port=DATABASE['port'],
            database='postgres',
            user=DATABASE['user'],
            password=DATABASE['password']
        )
        conn.autocommit = True
        cursor = conn.cursor()
        
        # Check if database exists
        cursor.execute("SELECT 1 FROM pg_database WHERE datname = 'email_master'")
        if not cursor.fetchone():
            cursor.execute('CREATE DATABASE email_master')
            logger.info("Created database 'email_master'")
        else:
            logger.info("Database 'email_master' already exists")
        
        cursor.close()
        conn.close()
        return True
        
    except Exception as e:
        logger.error(f"Failed to create database: {e}")
        return False


def create_tables() -> bool:
    """Create the required tables."""
    
    create_emails_table = """
    CREATE TABLE IF NOT EXISTS emails (
        id BIGSERIAL PRIMARY KEY,
        email VARCHAR(320) UNIQUE NOT NULL,
        email_domain VARCHAR(255),
        email_provider VARCHAR(50),
        email_brand VARCHAR(50),
        first_name VARCHAR(100),
        last_name VARCHAR(100),
        address TEXT,
        city VARCHAR(100),
        state VARCHAR(50),
        zipcode VARCHAR(20),
        phone VARCHAR(50),
        dob VARCHAR(50),
        gender VARCHAR(10),
        signup_date VARCHAR(50),
        signup_domain VARCHAR(255),
        signup_ip VARCHAR(45),
        is_clicker BOOLEAN DEFAULT FALSE,
        is_opener BOOLEAN DEFAULT FALSE,
        validation_status VARCHAR(50),
        email_category VARCHAR(50),
        quality_score SMALLINT,
        data_source VARCHAR(100),
        country VARCHAR(100),
        file_sources TEXT[],
        custom1 TEXT,
        custom2 TEXT,
        custom3 TEXT,
        custom4 TEXT,
        custom5 TEXT,
        created_at TIMESTAMP DEFAULT NOW(),
        updated_at TIMESTAMP DEFAULT NOW()
    );
    """
    
    create_stats_table = """
    CREATE TABLE IF NOT EXISTS processing_stats (
        id SERIAL PRIMARY KEY,
        run_date TIMESTAMP DEFAULT NOW(),
        files_processed INT,
        total_records_read BIGINT,
        records_loaded BIGINT,
        duplicates_found BIGINT,
        invalid_emails BIGINT,
        role_emails_filtered BIGINT,
        country_tld_filtered BIGINT,
        processing_time_seconds INT,
        notes TEXT
    );
    """
    
    create_file_log_table = """
    CREATE TABLE IF NOT EXISTS file_processing_log (
        id SERIAL PRIMARY KEY,
        filename VARCHAR(500),
        filepath TEXT,
        file_date TIMESTAMP,
        records_in_file BIGINT,
        records_processed BIGINT,
        records_loaded BIGINT,
        schema_detected VARCHAR(50),
        data_source VARCHAR(100),
        is_clicker_file BOOLEAN DEFAULT FALSE,
        is_opener_file BOOLEAN DEFAULT FALSE,
        processing_time_seconds INT,
        processed_at TIMESTAMP DEFAULT NOW(),
        errors TEXT
    );
    """
    
    try:
        with get_cursor() as cursor:
            cursor.execute(create_emails_table)
            cursor.execute(create_stats_table)
            cursor.execute(create_file_log_table)
            logger.info("Database tables created successfully")
        return True
        
    except Exception as e:
        logger.error(f"Failed to create tables: {e}")
        return False


def create_indexes() -> bool:
    """Create performance indexes after data load."""
    
    indexes = [
        "CREATE INDEX IF NOT EXISTS idx_email_domain ON emails (email_domain)",
        "CREATE INDEX IF NOT EXISTS idx_email_provider ON emails (email_provider)",
        "CREATE INDEX IF NOT EXISTS idx_email_brand ON emails (email_brand)",
        "CREATE INDEX IF NOT EXISTS idx_category ON emails (email_category)",
        "CREATE INDEX IF NOT EXISTS idx_quality_score ON emails (quality_score)",
        "CREATE INDEX IF NOT EXISTS idx_data_source ON emails (data_source)",
        "CREATE INDEX IF NOT EXISTS idx_clicker ON emails (is_clicker) WHERE is_clicker = TRUE",
        "CREATE INDEX IF NOT EXISTS idx_opener ON emails (is_opener) WHERE is_opener = TRUE",
        "CREATE INDEX IF NOT EXISTS idx_validation ON emails (validation_status)",
        "CREATE INDEX IF NOT EXISTS idx_signup_date ON emails (signup_date)",
        "CREATE INDEX IF NOT EXISTS idx_state ON emails (state)",
        "CREATE INDEX IF NOT EXISTS idx_city_state ON emails (city, state)",
        "CREATE INDEX IF NOT EXISTS idx_high_quality ON emails (quality_score) WHERE quality_score >= 70",
    ]
    
    try:
        with get_cursor() as cursor:
            for idx_sql in indexes:
                logger.info(f"Creating index: {idx_sql[:60]}...")
                cursor.execute(idx_sql)
        logger.info("All indexes created successfully")
        return True
        
    except Exception as e:
        logger.error(f"Failed to create indexes: {e}")
        return False


def drop_indexes() -> bool:
    """Drop indexes before bulk load for better performance."""
    
    indexes = [
        "DROP INDEX IF EXISTS idx_email_domain",
        "DROP INDEX IF EXISTS idx_email_provider",
        "DROP INDEX IF EXISTS idx_email_brand",
        "DROP INDEX IF EXISTS idx_category",
        "DROP INDEX IF EXISTS idx_quality_score",
        "DROP INDEX IF EXISTS idx_data_source",
        "DROP INDEX IF EXISTS idx_clicker",
        "DROP INDEX IF EXISTS idx_opener",
        "DROP INDEX IF EXISTS idx_validation",
        "DROP INDEX IF EXISTS idx_signup_date",
        "DROP INDEX IF EXISTS idx_state",
        "DROP INDEX IF EXISTS idx_city_state",
        "DROP INDEX IF EXISTS idx_high_quality",
    ]
    
    try:
        with get_cursor() as cursor:
            for idx_sql in indexes:
                cursor.execute(idx_sql)
        logger.info("Indexes dropped for bulk loading")
        return True
        
    except Exception as e:
        logger.error(f"Failed to drop indexes: {e}")
        return False


# =============================================================================
# BULK OPERATIONS
# =============================================================================

def bulk_insert_emails(records: List[Dict[str, Any]], on_conflict: str = 'skip') -> int:
    """
    Bulk insert email records using COPY for maximum performance.
    
    Args:
        records: List of email record dictionaries
        on_conflict: 'skip' to ignore duplicates, 'update' to update existing
        
    Returns:
        Number of records inserted
    """
    if not records:
        return 0
    
    # Define columns in order
    columns = [
        'email', 'email_domain', 'email_provider', 'email_brand',
        'first_name', 'last_name', 'address',
        'city', 'state', 'zipcode', 'phone', 'dob', 'gender',
        'signup_date', 'signup_domain', 'signup_ip',
        'is_clicker', 'is_opener', 'validation_status',
        'email_category', 'quality_score', 'data_source', 'country', 'file_sources',
        'custom1', 'custom2', 'custom3', 'custom4', 'custom5'
    ]
    
    # Create a StringIO buffer for COPY
    buffer = io.StringIO()
    writer = csv.writer(buffer, delimiter='\t', quoting=csv.QUOTE_MINIMAL)
    
    for record in records:
        row = []
        for col in columns:
            value = record.get(col)
            if value is None:
                row.append('\\N')  # NULL in COPY format
            elif col == 'file_sources' and isinstance(value, list):
                # Convert list to PostgreSQL array format - simple format
                cleaned = [str(v).replace(',', '_').replace('{', '').replace('}', '').replace('"', '') for v in value]
                row.append('{' + ','.join(cleaned) + '}')
            elif isinstance(value, bool):
                row.append('t' if value else 'f')
            else:
                row.append(str(value).replace('\t', ' ').replace('\n', ' '))
        writer.writerow(row)
    
    buffer.seek(0)
    
    try:
        with get_connection() as conn:
            cursor = conn.cursor()
            
            # Create temp table
            cursor.execute("""
                CREATE TEMP TABLE temp_emails (LIKE emails INCLUDING ALL)
                ON COMMIT DROP
            """)
            
            # COPY into temp table
            cursor.copy_from(
                buffer,
                'temp_emails',
                columns=columns,
                null='\\N'
            )
            
            # Insert from temp table, handling conflicts
            if on_conflict == 'skip':
                cursor.execute("""
                    INSERT INTO emails (
                        email, email_domain, email_provider, email_brand,
                        first_name, last_name, address,
                        city, state, zipcode, phone, dob, gender,
                        signup_date, signup_domain, signup_ip,
                        is_clicker, is_opener, validation_status,
                        email_category, quality_score, data_source, country, file_sources,
                        custom1, custom2, custom3, custom4, custom5
                    )
                    SELECT 
                        email, email_domain, email_provider, email_brand,
                        first_name, last_name, address,
                        city, state, zipcode, phone, dob, gender,
                        signup_date, signup_domain, signup_ip,
                        is_clicker, is_opener, validation_status,
                        email_category, quality_score, data_source, country, file_sources,
                        custom1, custom2, custom3, custom4, custom5
                    FROM temp_emails
                    ON CONFLICT (email) DO NOTHING
                """)
            else:
                cursor.execute("""
                    INSERT INTO emails (
                        email, email_domain, email_provider, email_brand,
                        first_name, last_name, address,
                        city, state, zipcode, phone, dob, gender,
                        signup_date, signup_domain, signup_ip,
                        is_clicker, is_opener, validation_status,
                        email_category, quality_score, data_source, country, file_sources,
                        custom1, custom2, custom3, custom4, custom5
                    )
                    SELECT 
                        email, email_domain, email_provider, email_brand,
                        first_name, last_name, address,
                        city, state, zipcode, phone, dob, gender,
                        signup_date, signup_domain, signup_ip,
                        is_clicker, is_opener, validation_status,
                        email_category, quality_score, data_source, country, file_sources,
                        custom1, custom2, custom3, custom4, custom5
                    FROM temp_emails
                    ON CONFLICT (email) DO UPDATE SET
                        first_name = COALESCE(EXCLUDED.first_name, emails.first_name),
                        last_name = COALESCE(EXCLUDED.last_name, emails.last_name),
                        address = COALESCE(EXCLUDED.address, emails.address),
                        city = COALESCE(EXCLUDED.city, emails.city),
                        state = COALESCE(EXCLUDED.state, emails.state),
                        zipcode = COALESCE(EXCLUDED.zipcode, emails.zipcode),
                        phone = COALESCE(EXCLUDED.phone, emails.phone),
                        is_clicker = emails.is_clicker OR EXCLUDED.is_clicker,
                        is_opener = emails.is_opener OR EXCLUDED.is_opener,
                        quality_score = GREATEST(COALESCE(emails.quality_score, 0), COALESCE(EXCLUDED.quality_score, 0)),
                        file_sources = array_cat(emails.file_sources, EXCLUDED.file_sources),
                        updated_at = NOW()
                """)
            
            inserted = cursor.rowcount
            cursor.close()
            conn.commit()
            return inserted
            
    except Exception as e:
        logger.error(f"Bulk insert failed: {e}")
        raise


def get_email_count() -> int:
    """Get total count of emails in database."""
    with get_cursor() as cursor:
        cursor.execute("SELECT COUNT(*) FROM emails")
        return cursor.fetchone()[0]


def get_category_counts() -> Dict[str, int]:
    """Get counts by email category."""
    with get_cursor() as cursor:
        cursor.execute("""
            SELECT email_category, COUNT(*) 
            FROM emails 
            GROUP BY email_category
            ORDER BY COUNT(*) DESC
        """)
        return {row[0]: row[1] for row in cursor.fetchall()}


def get_source_counts() -> Dict[str, int]:
    """Get counts by data source."""
    with get_cursor() as cursor:
        cursor.execute("""
            SELECT data_source, COUNT(*) 
            FROM emails 
            GROUP BY data_source
            ORDER BY COUNT(*) DESC
        """)
        return {row[0]: row[1] for row in cursor.fetchall()}


def get_provider_counts() -> Dict[str, int]:
    """Get counts by email provider (who hosts the email)."""
    with get_cursor() as cursor:
        cursor.execute("""
            SELECT email_provider, COUNT(*) 
            FROM emails 
            GROUP BY email_provider
            ORDER BY COUNT(*) DESC
        """)
        return {row[0]: row[1] for row in cursor.fetchall()}


def get_brand_counts() -> Dict[str, int]:
    """Get counts by email brand."""
    with get_cursor() as cursor:
        cursor.execute("""
            SELECT email_brand, COUNT(*) 
            FROM emails 
            GROUP BY email_brand
            ORDER BY COUNT(*) DESC
        """)
        return {row[0]: row[1] for row in cursor.fetchall()}


def get_quality_distribution() -> Dict[str, int]:
    """Get counts by quality score ranges."""
    with get_cursor() as cursor:
        cursor.execute("""
            SELECT 
                CASE 
                    WHEN quality_score >= 80 THEN 'High (80-100)'
                    WHEN quality_score >= 60 THEN 'Good (60-79)'
                    WHEN quality_score >= 40 THEN 'Average (40-59)'
                    WHEN quality_score >= 20 THEN 'Low (20-39)'
                    ELSE 'Poor (0-19)'
                END as quality_range,
                COUNT(*)
            FROM emails 
            GROUP BY quality_range
            ORDER BY MIN(quality_score) DESC
        """)
        return {row[0]: row[1] for row in cursor.fetchall()}


def log_file_processing(
    filename: str,
    filepath: str,
    file_date,
    records_in_file: int,
    records_processed: int,
    records_loaded: int,
    schema_detected: str,
    data_source: str,
    is_clicker: bool,
    is_opener: bool,
    processing_time: int,
    errors: str = None
) -> None:
    """Log file processing to database."""
    
    with get_cursor() as cursor:
        cursor.execute("""
            INSERT INTO file_processing_log (
                filename, filepath, file_date, records_in_file,
                records_processed, records_loaded, schema_detected,
                data_source, is_clicker_file, is_opener_file,
                processing_time_seconds, errors
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            filename, filepath, file_date, records_in_file,
            records_processed, records_loaded, schema_detected,
            data_source, is_clicker, is_opener, processing_time, errors
        ))


def log_processing_stats(
    files_processed: int,
    total_records_read: int,
    records_loaded: int,
    duplicates_found: int,
    invalid_emails: int,
    role_emails_filtered: int,
    country_tld_filtered: int,
    processing_time_seconds: int,
    notes: str = None
) -> None:
    """Log overall processing statistics."""
    
    with get_cursor() as cursor:
        cursor.execute("""
            INSERT INTO processing_stats (
                files_processed, total_records_read, records_loaded,
                duplicates_found, invalid_emails, role_emails_filtered,
                country_tld_filtered, processing_time_seconds, notes
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            files_processed, total_records_read, records_loaded,
            duplicates_found, invalid_emails, role_emails_filtered,
            country_tld_filtered, processing_time_seconds, notes
        ))


# =============================================================================
# INITIALIZATION
# =============================================================================

def initialize_database() -> bool:
    """Initialize the complete database setup."""
    
    logger.info("Initializing database...")
    
    if not create_database():
        return False
    
    # Initialize connection pool to the new database
    init_connection_pool()
    
    if not create_tables():
        return False
    
    logger.info("Database initialization complete")
    return True
