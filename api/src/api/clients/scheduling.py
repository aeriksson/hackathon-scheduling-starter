from typing import List, Optional, Dict, Any
import time
import json
import uuid
from datetime import datetime
import psycopg
from psycopg.rows import dict_row

from ..utils import log

logger = log.get_logger(__name__)

class SchedulingClient:
    def __init__(
        self,
        hostname: str = None,
        port: int = 5432,
        db: str = None,
        username: str = None,
        password: str = None,
        schema: str = "scheduling"
    ):
        self.hostname = hostname
        self.port = port
        self.db = db
        self.username = username
        self.password = password
        self.schema = schema
        self.conn = None
        self._is_query_service_ready = False
        self._tables_created = False

    def connect(self, max_retries: int = 30, initial_delay: float = 1.0, max_delay: float = 10.0) -> None:
        """
        Establish connection to PostgreSQL database.

        Args:
            max_retries: Maximum number of retry attempts.
            initial_delay: Initial delay between retries in seconds.
            max_delay: Maximum delay between retries in seconds.
        """
        try:
            self.conn = psycopg.connect(
                host=self.hostname,
                port=self.port,
                user=self.username,
                password=self.password,
                dbname=self.db,
                autocommit=True
            )

            try:
                self.init()
            except Exception as init_err:
                logger.warning(f"Tables not ready yet: {str(init_err)}")

            logger.info("Connected to PostgreSQL database")
        except Exception as conn_err:
            logger.warning(f"Database connection failed: {str(conn_err)}")
            raise

    def init(self, max_retries: int = 30, initial_delay: float = 1.0, max_delay: float = 10.0) -> None:
        """
        Create the tables if they don't exist.

        Args:
            max_retries: Maximum number of retry attempts.
            initial_delay: Initial delay between retries in seconds.
            max_delay: Maximum delay between retries in seconds.
        """
        # Add retry mechanism around cluster connection
        connection_delay = initial_delay
        connection_attempts = 0

        while not self.conn and connection_attempts < max_retries:
            try:
                self.connect()
                logger.info("Successfully connected to database")
            except Exception as e:
                connection_attempts += 1
                logger.warning(f"Failed to connect to database (attempt {connection_attempts}/{max_retries}): {str(e)}")
                if connection_attempts < max_retries:
                    logger.info(f"Retrying connection in {connection_delay:.1f} seconds...")
                    time.sleep(connection_delay)
                    # Exponential backoff with a cap
                    connection_delay = min(max_delay, connection_delay * 1.5)
                else:
                    logger.error(f"Failed to connect to database after {max_retries} attempts")
                    raise

        if self._tables_created:
            return

        try:
            with self.conn.cursor() as cursor:
                # Create schema if it doesn't exist
                cursor.execute(f"""
                CREATE SCHEMA IF NOT EXISTS {self.schema}
                """)

                # Create employees table
                cursor.execute(f"""
                CREATE TABLE IF NOT EXISTS {self.schema}.employees (
                    employee_number VARCHAR(36) PRIMARY KEY,
                    name VARCHAR(255) NOT NULL,
                    first_line_support_count INT NOT NULL DEFAULT 0,
                    known_absences JSONB NOT NULL DEFAULT '[]',
                    metadata JSONB NOT NULL DEFAULT '{{}}'
                )
                """)

                # Create schedules table
                cursor.execute(f"""
                CREATE TABLE IF NOT EXISTS {self.schema}.schedules (
                    date VARCHAR(10) PRIMARY KEY,
                    first_line_support VARCHAR(36) NOT NULL,
                    FOREIGN KEY (first_line_support) REFERENCES {self.schema}.employees(employee_number)
                )
                """)

                # Create rules table
                cursor.execute(f"""
                CREATE TABLE IF NOT EXISTS {self.schema}.rules (
                    id VARCHAR(36) PRIMARY KEY,
                    max_days_per_week INT NOT NULL,
                    preferred_balance FLOAT NOT NULL
                )
                """)

                self._tables_created = True
                logger.info("Database tables initialized successfully")

                # Initialize default rules if not exists
                self._init_default_rules()
        except Exception as e:
            logger.error(f"Error initializing tables: {str(e)}")
            raise

    def _init_default_rules(self) -> None:
        """Initialize default rules if they don't exist."""
        try:
            with self.conn.cursor(row_factory=dict_row) as cursor:
                cursor.execute(f"""
                SELECT * FROM {self.schema}.rules WHERE id = 'system_rules'
                """)
                existing_rules = cursor.fetchone()

                if not existing_rules:
                    cursor.execute(f"""
                    INSERT INTO {self.schema}.rules (id, max_days_per_week, preferred_balance)
                    VALUES ('system_rules', 3, 0.2)
                    """)
                    logger.info("Initialized default scheduling rules")
        except Exception as e:
            logger.warning(f"Failed to initialize default rules: {str(e)}")

    def await_up(self, max_retries: int = 30, initial_delay: float = 1.0, max_delay: float = 10.0) -> None:
        """
        Wait until the PostgreSQL database is available by running a simple query in a loop.

        Args:
            max_retries: Maximum number of retry attempts.
            initial_delay: Initial delay between retries in seconds.
            max_delay: Maximum delay between retries in seconds.
        """
        # If we already know the service is ready, skip the check
        if self._is_query_service_ready:
            return

        if not self.conn:
            self.connect()

        delay = initial_delay
        for attempt in range(1, max_retries + 1):
            try:
                with self.conn.cursor() as cursor:
                    # Try a simple query
                    cursor.execute("SELECT 1")
                    cursor.fetchone()

                # If we got here, the database is ready
                self._is_query_service_ready = True
                logger.info("PostgreSQL database is ready")
                return
            except Exception as e:
                logger.warning(
                    f"Attempt {attempt}/{max_retries}: PostgreSQL database not available yet. "
                    f"Retrying in {delay:.1f} seconds... Error: {str(e)}"
                )
                time.sleep(delay)
                # Exponential backoff with a cap
                delay = min(max_delay, delay * 1.5)

                # Try to reconnect
                try:
                    if self.conn.closed:
                        self.connect()
                except Exception:
                    pass

        # If we've exhausted all retries
        raise Exception(f"PostgreSQL database not available after {max_retries} attempts")

    # Employee methods
    def create_employee(self, name: str, employee_number: str, known_absences: List[str] = None, metadata: Dict[str, Any] = None) -> str:
        """
        Create a new employee.

        Args:
            name: The name of the employee
            employee_number: The employee number (unique identifier)
            known_absences: Optional list of known absence dates in ISO format
            metadata: Optional metadata for the employee

        Returns:
            The employee number
        """
        if not self.conn or self.conn.closed:
            self.connect()

        known_absences = known_absences or []
        metadata = metadata or {}

        try:
            with self.conn.cursor() as cursor:
                cursor.execute(
                    f"""
                    INSERT INTO {self.schema}.employees 
                    (employee_number, name, first_line_support_count, known_absences, metadata)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (employee_number) DO UPDATE SET
                        name = EXCLUDED.name,
                        known_absences = EXCLUDED.known_absences,
                        metadata = EXCLUDED.metadata
                    """,
                    (
                        employee_number,
                        name,
                        0,
                        json.dumps(known_absences),
                        json.dumps(metadata)
                    )
                )
            logger.info(f"Created employee with number: {employee_number}")
            return employee_number
        except Exception:
            logger.exception("Failed to create employee")
            raise

    def get_employee(self, employee_number: str) -> Optional[Dict[str, Any]]:
        """
        Get an employee by employee number.

        Args:
            employee_number: The employee number

        Returns:
            The employee details or None if not found
        """
        if not self.conn or self.conn.closed:
            self.connect()

        try:
            with self.conn.cursor(row_factory=dict_row) as cursor:
                cursor.execute(
                    f"""
                    SELECT employee_number, name, first_line_support_count, 
                           known_absences, metadata
                    FROM {self.schema}.employees
                    WHERE employee_number = %s
                    """,
                    (employee_number,)
                )
                result = cursor.fetchone()

                if not result:
                    return None

                # Parse JSON fields
                employee = dict(result)
                if isinstance(employee["known_absences"], str):
                    employee["known_absences"] = json.loads(employee["known_absences"])
                if isinstance(employee["metadata"], str):
                    employee["metadata"] = json.loads(employee["metadata"])

                return employee
        except Exception as e:
            logger.warning(f"Failed to get employee: {str(e)}")
            return None

    def get_employees(self) -> List[Dict[str, Any]]:
        """
        Get all employees.

        Returns:
            List of employees
        """
        if not self.conn or self.conn.closed:
            self.connect()

        # Make sure the database is available
        self.await_up()

        try:
            with self.conn.cursor(row_factory=dict_row) as cursor:
                cursor.execute(
                    f"""
                    SELECT employee_number, name, first_line_support_count, 
                           known_absences, metadata
                    FROM {self.schema}.employees
                    """
                )
                employees = []
                for row in cursor.fetchall():
                    employee = dict(row)
                    
                    # Parse JSON fields
                    if isinstance(employee["known_absences"], str):
                        employee["known_absences"] = json.loads(employee["known_absences"])
                    if isinstance(employee["metadata"], str):
                        employee["metadata"] = json.loads(employee["metadata"])
                    
                    employees.append(employee)
                
                return employees
        except Exception:
            logger.exception("Failed to get employees.")
            raise

    def update_employee(self, employee_number: str, updates: Dict[str, Any]) -> bool:
        """
        Update an employee.

        Args:
            employee_number: The employee number
            updates: The fields to update

        Returns:
            True if the update was successful, False otherwise
        """
        if not self.conn or self.conn.closed:
            self.connect()

        try:
            employee = self.get_employee(employee_number)
            if not employee:
                return False

            # Update employee fields
            for key, value in updates.items():
                employee[key] = value

            # Handle JSON fields
            known_absences = json.dumps(employee["known_absences"]) if isinstance(employee["known_absences"], list) else employee["known_absences"]
            metadata = json.dumps(employee["metadata"]) if isinstance(employee["metadata"], dict) else employee["metadata"]

            with self.conn.cursor() as cursor:
                cursor.execute(
                    f"""
                    UPDATE {self.schema}.employees
                    SET name = %s,
                        first_line_support_count = %s,
                        known_absences = %s,
                        metadata = %s
                    WHERE employee_number = %s
                    """,
                    (
                        employee["name"],
                        employee["first_line_support_count"],
                        known_absences,
                        metadata,
                        employee_number
                    )
                )
            logger.info(f"Updated employee {employee_number}")
            return True
        except Exception:
            logger.exception("Failed to update employee")
            return False

    def delete_employee(self, employee_number: str) -> bool:
        """
        Delete an employee.

        Args:
            employee_number: The employee number

        Returns:
            True if the employee was deleted, False otherwise
        """
        if not self.conn or self.conn.closed:
            self.connect()

        try:
            employee = self.get_employee(employee_number)
            if not employee:
                return False

            with self.conn.cursor() as cursor:
                # First update any schedules that reference this employee
                cursor.execute(
                    f"""
                    SELECT date FROM {self.schema}.schedules 
                    WHERE first_line_support = %s
                    """,
                    (employee_number,)
                )
                affected_schedules = [row[0] for row in cursor.fetchall()]
                
                # Delete schedules that reference this employee
                # Note: In a production system, you might want to reassign these instead
                if affected_schedules:
                    cursor.execute(
                        f"""
                        DELETE FROM {self.schema}.schedules 
                        WHERE first_line_support = %s
                        """,
                        (employee_number,)
                    )
                
                # Now delete the employee
                cursor.execute(
                    f"""
                    DELETE FROM {self.schema}.employees
                    WHERE employee_number = %s
                    """,
                    (employee_number,)
                )
                
                rows_deleted = cursor.rowcount

            logger.info(f"Deleted employee {employee_number}")
            return rows_deleted > 0
        except Exception:
            logger.exception("Failed to delete employee")
            return False

    # Schedule methods
    def create_schedule(self, date_str: str, employee_number: str) -> str:
        """
        Create a schedule entry.

        Args:
            date_str: The date in ISO format (YYYY-MM-DD)
            employee_number: The employee number for first-line support

        Returns:
            The schedule ID (date string)
        """
        if not self.conn or self.conn.closed:
            self.connect()

        try:
            with self.conn.cursor() as cursor:
                cursor.execute(
                    f"""
                    INSERT INTO {self.schema}.schedules
                    (date, first_line_support)
                    VALUES (%s, %s)
                    ON CONFLICT (date) DO UPDATE SET
                        first_line_support = EXCLUDED.first_line_support
                    """,
                    (
                        date_str,
                        employee_number
                    )
                )
            logger.info(f"Created schedule for date: {date_str}")

            # Update employee's first-line support count
            self._update_employee_counts()

            return date_str
        except Exception:
            logger.exception("Failed to create schedule")
            raise

    def get_schedule(self, date_str: str) -> Optional[Dict[str, Any]]:
        """
        Get a schedule by date.

        Args:
            date_str: The date in ISO format (YYYY-MM-DD)

        Returns:
            The schedule details or None if not found
        """
        if not self.conn or self.conn.closed:
            self.connect()

        try:
            with self.conn.cursor(row_factory=dict_row) as cursor:
                cursor.execute(
                    f"""
                    SELECT date, first_line_support
                    FROM {self.schema}.schedules
                    WHERE date = %s
                    """,
                    (date_str,)
                )
                result = cursor.fetchone()

                if not result:
                    return None

                return dict(result)
        except Exception as e:
            logger.warning(f"Failed to get schedule: {str(e)}")
            return None

    def get_schedules(self, start_date: str = None, end_date: str = None) -> List[Dict[str, Any]]:
        """
        Get schedules within a date range.

        Args:
            start_date: Optional start date in ISO format (inclusive)
            end_date: Optional end date in ISO format (inclusive)

        Returns:
            List of schedules
        """
        if not self.conn or self.conn.closed:
            self.connect()

        # Make sure the database is available
        self.await_up()

        try:
            with self.conn.cursor(row_factory=dict_row) as cursor:
                query = f"""
                SELECT date, first_line_support
                FROM {self.schema}.schedules
                """
                
                params = []
                where_clauses = []
                
                if start_date:
                    where_clauses.append("date >= %s")
                    params.append(start_date)
                
                if end_date:
                    where_clauses.append("date <= %s")
                    params.append(end_date)
                
                if where_clauses:
                    query += " WHERE " + " AND ".join(where_clauses)
                
                query += " ORDER BY date ASC"
                
                cursor.execute(query, params)
                
                return [dict(row) for row in cursor.fetchall()]
        except Exception:
            logger.exception("Failed to get schedules.")
            raise

    def update_schedule(self, date_str: str, employee_number: str) -> bool:
        """
        Update a schedule entry.

        Args:
            date_str: The date in ISO format (YYYY-MM-DD)
            employee_number: The new employee number for first-line support

        Returns:
            True if the update was successful, False otherwise
        """
        if not self.conn or self.conn.closed:
            self.connect()

        try:
            schedule = self.get_schedule(date_str)
            if not schedule:
                return False

            with self.conn.cursor() as cursor:
                cursor.execute(
                    f"""
                    UPDATE {self.schema}.schedules
                    SET first_line_support = %s
                    WHERE date = %s
                    """,
                    (employee_number, date_str)
                )
                rows_updated = cursor.rowcount

            if rows_updated > 0:
                logger.info(f"Updated schedule for date {date_str}")
                
                # Update employee counts
                self._update_employee_counts()
                
                return True
            return False
        except Exception:
            logger.exception("Failed to update schedule")
            return False

    def delete_schedule(self, date_str: str) -> bool:
        """
        Delete a schedule entry.

        Args:
            date_str: The date in ISO format (YYYY-MM-DD)

        Returns:
            True if the schedule was deleted, False otherwise
        """
        if not self.conn or self.conn.closed:
            self.connect()

        try:
            schedule = self.get_schedule(date_str)
            if not schedule:
                return False

            with self.conn.cursor() as cursor:
                cursor.execute(
                    f"""
                    DELETE FROM {self.schema}.schedules
                    WHERE date = %s
                    """,
                    (date_str,)
                )
                rows_deleted = cursor.rowcount

            if rows_deleted > 0:
                logger.info(f"Deleted schedule for date {date_str}")
                
                # Update employee counts
                self._update_employee_counts()
                
                return True
            return False
        except Exception:
            logger.exception("Failed to delete schedule")
            return False

    def _update_employee_counts(self) -> None:
        """Update first-line support counts for all employees."""
        if not self.conn or self.conn.closed:
            self.connect()

        try:
            # First reset all counts to zero
            with self.conn.cursor() as cursor:
                cursor.execute(
                    f"""
                    UPDATE {self.schema}.employees
                    SET first_line_support_count = 0
                    """
                )
                
                # Then count schedules for each employee and update
                cursor.execute(
                    f"""
                    SELECT first_line_support, COUNT(*) as count
                    FROM {self.schema}.schedules
                    GROUP BY first_line_support
                    """
                )
                employee_counts = cursor.fetchall()
                
                # Update each employee with their count
                for emp_id, count in employee_counts:
                    cursor.execute(
                        f"""
                        UPDATE {self.schema}.employees
                        SET first_line_support_count = %s
                        WHERE employee_number = %s
                        """,
                        (count, emp_id)
                    )
                    
            logger.info("Updated employee first-line support counts")
        except Exception:
            logger.exception("Failed to update employee counts")

    # Rules methods
    def get_rules(self) -> Dict[str, Any]:
        """
        Get the scheduling system rules.

        Returns:
            The rules
        """
        if not self.conn or self.conn.closed:
            self.connect()

        try:
            with self.conn.cursor(row_factory=dict_row) as cursor:
                cursor.execute(
                    f"""
                    SELECT max_days_per_week, preferred_balance
                    FROM {self.schema}.rules
                    WHERE id = 'system_rules'
                    """
                )
                result = cursor.fetchone()

                if not result:
                    # Initialize default rules if not found
                    self._init_default_rules()
                    cursor.execute(
                        f"""
                        SELECT max_days_per_week, preferred_balance
                        FROM {self.schema}.rules
                        WHERE id = 'system_rules'
                        """
                    )
                    result = cursor.fetchone()

                # Return default rules if still not found
                if not result:
                    return {
                        "max_days_per_week": 3,
                        "preferred_balance": 0.2
                    }

                return dict(result)
        except Exception as e:
            logger.warning(f"Failed to get rules: {str(e)}")
            # Return default rules
            return {
                "max_days_per_week": 3,
                "preferred_balance": 0.2
            }

    def update_rules(self, updates: Dict[str, Any]) -> bool:
        """
        Update the scheduling system rules.

        Args:
            updates: The rule fields to update

        Returns:
            True if the update was successful, False otherwise
        """
        if not self.conn or self.conn.closed:
            self.connect()

        try:
            current_rules = self.get_rules()

            # Update rule fields (only if provided)
            for key, value in updates.items():
                if value is not None:
                    current_rules[key] = value

            with self.conn.cursor() as cursor:
                cursor.execute(
                    f"""
                    UPDATE {self.schema}.rules
                    SET max_days_per_week = %s,
                        preferred_balance = %s
                    WHERE id = 'system_rules'
                    """,
                    (
                        current_rules["max_days_per_week"],
                        current_rules["preferred_balance"]
                    )
                )
                rows_updated = cursor.rowcount

                # If no rows were updated, the rules might not exist yet
                if rows_updated == 0:
                    cursor.execute(
                        f"""
                        INSERT INTO {self.schema}.rules (id, max_days_per_week, preferred_balance)
                        VALUES ('system_rules', %s, %s)
                        """,
                        (
                            current_rules["max_days_per_week"],
                            current_rules["preferred_balance"]
                        )
                    )

            logger.info("Updated scheduling rules")
            return True
        except Exception:
            logger.exception("Failed to update rules")
            return False

    def close(self) -> None:
        """Close the database connection."""
        if self.conn and not self.conn.closed:
            self.conn.close()
            logger.info("Database connection closed")

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()