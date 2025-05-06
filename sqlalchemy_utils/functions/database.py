import itertools
import os
from collections.abc import Mapping, Sequence
from copy import copy

import sqlalchemy as sa
from sqlalchemy.engine.url import make_url
from sqlalchemy.exc import OperationalError, ProgrammingError

from ..utils import starts_with
from .orm import quote


def escape_like(string, escape_char='*'):
    """
    Escape the string parameter used in SQL LIKE expressions.

    ::

        from sqlalchemy_utils import escape_like


        query = session.query(User).filter(
            User.name.ilike(escape_like('John'))
        )


    :param string: a string to escape
    :param escape_char: escape character
    """
    return (
        string
        .replace(escape_char, escape_char * 2)
        .replace('%', escape_char + '%')
        .replace('_', escape_char + '_')
    )


def json_sql(value, scalars_to_json=True):
    """
    Convert python data structures to PostgreSQL specific SQLAlchemy JSON
    constructs. This function is extremly useful if you need to build
    PostgreSQL JSON on python side.

    .. note::

        This function needs PostgreSQL >= 9.4

    Scalars are converted to to_json SQLAlchemy function objects

    ::

        json_sql(1)     # Equals SQL: to_json(1)

        json_sql('a')   # to_json('a')


    Mappings are converted to json_build_object constructs

    ::

        json_sql({'a': 'c', '2': 5})  # json_build_object('a', 'c', '2', 5)


    Sequences (other than strings) are converted to json_build_array constructs

    ::

        json_sql([1, 2, 3])  # json_build_array(1, 2, 3)


    You can also nest these data structures

    ::

        json_sql({'a': [1, 2, 3]})
        # json_build_object('a', json_build_array[1, 2, 3])


    :param value:
        value to be converted to SQLAlchemy PostgreSQL function constructs
    """
    if scalars_to_json:
        def scalar_convert(a):
            return sa.func.to_json(sa.text(a))
    else:
        scalar_convert = sa.text

    if isinstance(value, Mapping):
        return sa.func.json_build_object(
            *(
                json_sql(v, scalars_to_json=False)
                for v in itertools.chain(*value.items())
            )
        )
    elif isinstance(value, str):
        return scalar_convert(f"'{value}'")
    elif isinstance(value, Sequence):
        return sa.func.json_build_array(
            *(
                json_sql(v, scalars_to_json=False)
                for v in value
            )
        )
    elif isinstance(value, (int, float)):
        return scalar_convert(str(value))
    return value


def jsonb_sql(value, scalars_to_jsonb=True):
    """
    Convert python data structures to PostgreSQL specific SQLAlchemy JSONB
    constructs. This function is extremly useful if you need to build
    PostgreSQL JSONB on python side.

    .. note::

        This function needs PostgreSQL >= 9.4

    Scalars are converted to to_jsonb SQLAlchemy function objects

    ::

        jsonb_sql(1)     # Equals SQL: to_jsonb(1)

        jsonb_sql('a')   # to_jsonb('a')


    Mappings are converted to jsonb_build_object constructs

    ::

        jsonb_sql({'a': 'c', '2': 5})  # jsonb_build_object('a', 'c', '2', 5)


    Sequences (other than strings) converted to jsonb_build_array constructs

    ::

        jsonb_sql([1, 2, 3])  # jsonb_build_array(1, 2, 3)


    You can also nest these data structures

    ::

        jsonb_sql({'a': [1, 2, 3]})
        # jsonb_build_object('a', jsonb_build_array[1, 2, 3])


    :param value:
        value to be converted to SQLAlchemy PostgreSQL function constructs
    :boolean jsonbb:
        Flag to alternatively convert the return with a to_jsonb construct
    """
    if scalars_to_jsonb:
        def scalar_convert(a):
            return sa.func.to_jsonb(sa.text(a))
    else:
        scalar_convert = sa.text

    if isinstance(value, Mapping):
        return sa.func.jsonb_build_object(
            *(
                jsonb_sql(v, scalars_to_jsonb=False)
                for v in itertools.chain(*value.items())
            )
        )
    elif isinstance(value, str):
        return scalar_convert(f"'{value}'")
    elif isinstance(value, Sequence):
        return sa.func.jsonb_build_array(
            *(
                jsonb_sql(v, scalars_to_jsonb=False)
                for v in value
            )
        )
    elif isinstance(value, (int, float)):
        return scalar_convert(str(value))
    return value


def has_index(column_or_constraint):
    """
    Return whether or not given column or the columns of given foreign key
    constraint have an index. A column has an index if it has a single column
    index or it is the first column in compound column index.

    A foreign key constraint has an index if the constraint columns are the
    first columns in compound column index.

    :param column_or_constraint:
        SQLAlchemy Column object or SA ForeignKeyConstraint object

    .. versionadded: 0.26.2

    .. versionchanged: 0.30.18
        Added support for foreign key constaints.

    ::

        from sqlalchemy_utils import has_index


        class Article(Base):
            __tablename__ = 'article'
            id = sa.Column(sa.Integer, primary_key=True)
            title = sa.Column(sa.String(100))
            is_published = sa.Column(sa.Boolean, index=True)
            is_deleted = sa.Column(sa.Boolean)
            is_archived = sa.Column(sa.Boolean)

            __table_args__ = (
                sa.Index('my_index', is_deleted, is_archived),
            )


        table = Article.__table__

        has_index(table.c.is_published) # True
        has_index(table.c.is_deleted)   # True
        has_index(table.c.is_archived)  # False


    Also supports primary key indexes

    ::

        from sqlalchemy_utils import has_index


        class ArticleTranslation(Base):
            __tablename__ = 'article_translation'
            id = sa.Column(sa.Integer, primary_key=True)
            locale = sa.Column(sa.String(10), primary_key=True)
            title = sa.Column(sa.String(100))


        table = ArticleTranslation.__table__

        has_index(table.c.locale)   # False
        has_index(table.c.id)       # True


    This function supports foreign key constraints as well

    ::


        class User(Base):
            __tablename__ = 'user'
            first_name = sa.Column(sa.Unicode(255), primary_key=True)
            last_name = sa.Column(sa.Unicode(255), primary_key=True)

        class Article(Base):
            __tablename__ = 'article'
            id = sa.Column(sa.Integer, primary_key=True)
            author_first_name = sa.Column(sa.Unicode(255))
            author_last_name = sa.Column(sa.Unicode(255))
            __table_args__ = (
                sa.ForeignKeyConstraint(
                    [author_first_name, author_last_name],
                    [User.first_name, User.last_name]
                ),
                sa.Index(
                    'my_index',
                    author_first_name,
                    author_last_name
                )
            )

        table = Article.__table__
        constraint = list(table.foreign_keys)[0].constraint

        has_index(constraint)  # True
    """
    table = column_or_constraint.table
    if not isinstance(table, sa.Table):
        raise TypeError(
            'Only columns belonging to Table objects are supported. Given '
            'column belongs to %r.' % table
        )
    primary_keys = table.primary_key.columns.values()
    if isinstance(column_or_constraint, sa.ForeignKeyConstraint):
        columns = list(column_or_constraint.columns.values())
    else:
        columns = [column_or_constraint]

    return (
        (primary_keys and starts_with(primary_keys, columns)) or
        any(
            starts_with(index.columns.values(), columns)
            for index in table.indexes
        )
    )


def has_unique_index(column_or_constraint):
    """
    Return whether or not given column or given foreign key constraint has a
    unique index.

    A column has a unique index if it has a single column primary key index or
    it has a single column UniqueConstraint.

    A foreign key constraint has a unique index if the columns of the
    constraint are the same as the columns of table primary key or the coluns
    of any unique index or any unique constraint of the given table.

    :param column: SQLAlchemy Column object

    .. versionadded: 0.27.1

    .. versionchanged: 0.30.18
        Added support for foreign key constaints.

        Fixed support for unique indexes (previously only worked for unique
        constraints)

    ::

        from sqlalchemy_utils import has_unique_index


        class Article(Base):
            __tablename__ = 'article'
            id = sa.Column(sa.Integer, primary_key=True)
            title = sa.Column(sa.String(100))
            is_published = sa.Column(sa.Boolean, unique=True)
            is_deleted = sa.Column(sa.Boolean)
            is_archived = sa.Column(sa.Boolean)


        table = Article.__table__

        has_unique_index(table.c.is_published) # True
        has_unique_index(table.c.is_deleted)   # False
        has_unique_index(table.c.id)           # True


    This function supports foreign key constraints as well

    ::


        class User(Base):
            __tablename__ = 'user'
            first_name = sa.Column(sa.Unicode(255), primary_key=True)
            last_name = sa.Column(sa.Unicode(255), primary_key=True)

        class Article(Base):
            __tablename__ = 'article'
            id = sa.Column(sa.Integer, primary_key=True)
            author_first_name = sa.Column(sa.Unicode(255))
            author_last_name = sa.Column(sa.Unicode(255))
            __table_args__ = (
                sa.ForeignKeyConstraint(
                    [author_first_name, author_last_name],
                    [User.first_name, User.last_name]
                ),
                sa.Index(
                    'my_index',
                    author_first_name,
                    author_last_name,
                    unique=True
                )
            )

        table = Article.__table__
        constraint = list(table.foreign_keys)[0].constraint

        has_unique_index(constraint)  # True


    :raises TypeError: if given column does not belong to a Table object
    """
    table = column_or_constraint.table
    if not isinstance(table, sa.Table):
        raise TypeError(
            'Only columns belonging to Table objects are supported. Given '
            'column belongs to %r.' % table
        )
    primary_keys = list(table.primary_key.columns.values())
    if isinstance(column_or_constraint, sa.ForeignKeyConstraint):
        columns = list(column_or_constraint.columns.values())
    else:
        columns = [column_or_constraint]

    return (
        (columns == primary_keys) or
        any(
            columns == list(constraint.columns.values())
            for constraint in table.constraints
            if isinstance(constraint, sa.sql.schema.UniqueConstraint)
        ) or
        any(
            columns == list(index.columns.values())
            for index in table.indexes
            if index.unique
        )
    )


def is_auto_assigned_date_column(column):
    """
    Returns whether or not given SQLAlchemy Column object's is auto assigned
    DateTime or Date.

    :param column: SQLAlchemy Column object
    """
    return (
        (
            isinstance(column.type, sa.DateTime) or
            isinstance(column.type, sa.Date)
        ) and
        (
            column.default or
            column.server_default or
            column.onupdate or
            column.server_onupdate
        )
    )


def _set_url_database(url: sa.engine.url.URL, database):
    """Set the database of an engine URL.

    :param url: A SQLAlchemy engine URL.
    :param database: New database to set.

    """
    if hasattr(url, '_replace'):
        # Cannot use URL.set() as database may need to be set to None.
        ret = url._replace(database=database)
    else:  # SQLAlchemy <1.4
        url = copy(url)
        url.database = database
        ret = url
    assert ret.database == database, ret
    return ret


def _get_scalar_result(engine, sql):
    with engine.connect() as conn:
        return conn.scalar(sql)


def _sqlite_file_exists(database):
    if not os.path.isfile(database) or os.path.getsize(database) < 100:
        return False

    with open(database, 'rb') as f:
        header = f.read(100)

    return header[:16] == b'SQLite format 3\x00'


def database_exists(url):
    """Check if a database exists.

    :param url: A SQLAlchemy engine URL.

    Performs backend-specific testing to quickly determine if a database
    exists on the server. ::

        database_exists('postgresql://postgres@localhost/name')  #=> False
        create_database('postgresql://postgres@localhost/name')
        database_exists('postgresql://postgres@localhost/name')  #=> True

    Supports checking against a constructed URL as well. ::

        engine = create_engine('postgresql://postgres@localhost/name')
        database_exists(engine.url)  #=> False
        create_database(engine.url)
        database_exists(engine.url)  #=> True

    """

    url = make_url(url)
    database = url.database
    dialect_name = url.get_dialect().name
    engine = None
    try:
        if dialect_name == 'postgresql':
            text = "SELECT 1 FROM pg_database WHERE datname='%s'" % database
            for db in (database, 'postgres', 'template1', 'template0', None):
                url = _set_url_database(url, database=db)
                engine = sa.create_engine(url)
                try:
                    return bool(_get_scalar_result(engine, sa.text(text)))
                except (ProgrammingError, OperationalError):
                    pass
            return False

        elif dialect_name == 'mysql':
            url = _set_url_database(url, database=None)
            engine = sa.create_engine(url)
            text = ("SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA "
                    "WHERE SCHEMA_NAME = '%s'" % database)
            return bool(_get_scalar_result(engine, sa.text(text)))

        elif dialect_name == 'sqlite':
            url = _set_url_database(url, database=None)
            engine = sa.create_engine(url)
            if database:
                return database == ':memory:' or _sqlite_file_exists(database)
            else:
                # The default SQLAlchemy database is in memory, and :memory: is
                # not required, thus we should support that use case.
                return True
        else:
            text = 'SELECT 1'
            try:
                engine = sa.create_engine(url)
                return bool(_get_scalar_result(engine, sa.text(text)))
            except (ProgrammingError, OperationalError):
                return False
    finally:
        if engine:
            engine.dispose()


def create_database(url, encoding='utf8', template=None):
    """Issue the appropriate CREATE DATABASE statement.

    :param url: A SQLAlchemy engine URL.
    :param encoding: The encoding to create the database as.
    :param template:
        The name of the template from which to create the new database. At the
        moment only supported by PostgreSQL driver.

    To create a database, you can pass a simple URL that would have
    been passed to ``create_engine``. ::

        create_database('postgresql://postgres@localhost/name')

    You may also pass the url from an existing engine. ::

        create_database(engine.url)

    Has full support for mysql, postgres, and sqlite. In theory,
    other database engines should be supported.
    """

    url = make_url(url)
    database = url.database
    dialect_name = url.get_dialect().name
    dialect_driver = url.get_dialect().driver

    if dialect_name == 'postgresql':
        url = _set_url_database(url, database="postgres")
    elif dialect_name == 'mssql':
        url = _set_url_database(url, database="master")
    elif dialect_name == 'cockroachdb':
        url = _set_url_database(url, database="defaultdb")
    elif not dialect_name == 'sqlite':
        url = _set_url_database(url, database=None)

    if (dialect_name == 'mssql' and dialect_driver in {'pymssql', 'pyodbc'}) \
            or (dialect_name == 'postgresql' and dialect_driver in {
            'asyncpg', 'pg8000', 'psycopg', 'psycopg2', 'psycopg2cffi'}):
        engine = sa.create_engine(url, isolation_level='AUTOCOMMIT')
    else:
        engine = sa.create_engine(url)

    if dialect_name == 'postgresql':
        if not template:
            template = 'template1'

        with engine.begin() as conn:
            text = "CREATE DATABASE {} ENCODING '{}' TEMPLATE {}".format(
                quote(conn, database),
                encoding,
                quote(conn, template)
            )
            conn.execute(sa.text(text))

    elif dialect_name == 'mysql':
        with engine.begin() as conn:
            text = "CREATE DATABASE {} CHARACTER SET = '{}'".format(
                quote(conn, database),
                encoding
            )
            conn.execute(sa.text(text))

    elif dialect_name == 'sqlite' and database != ':memory:':
        if database:
            with engine.begin() as conn:
                conn.execute(sa.text('CREATE TABLE DB(id int)'))
                conn.execute(sa.text('DROP TABLE DB'))

    else:
        with engine.begin() as conn:
            text = f'CREATE DATABASE {quote(conn, database)}'
            conn.execute(sa.text(text))

    engine.dispose()

def drop_database(url):
    """Issue the appropriate DROP DATABASE statement with enhanced transaction diagnostics.

    :param url: A SQLAlchemy engine URL.

    Works similar to the :func:`create_database` method in that both url text
    and a constructed url are accepted.

    ::

        drop_database('postgresql://postgres@localhost/name')
        drop_database(engine.url)

    """

    url = make_url(url)
    database = url.database
    dialect_name = url.get_dialect().name
    dialect_driver = url.get_dialect().driver

    if dialect_name == 'postgresql':
        url = _set_url_database(url, database="postgres")
    elif dialect_name == 'mssql':
        url = _set_url_database(url, database="master")
    elif dialect_name == 'cockroachdb':
        url = _set_url_database(url, database="defaultdb")
    elif not dialect_name == 'sqlite':
        url = _set_url_database(url, database=None)

    if dialect_name == 'mssql' and dialect_driver in {'pymssql', 'pyodbc'}:
        engine = sa.create_engine(url, connect_args={'autocommit': True})
    elif dialect_name == 'postgresql' and dialect_driver in {
            'asyncpg', 'pg8000', 'psycopg', 'psycopg2', 'psycopg2cffi'}:
        engine = sa.create_engine(url, isolation_level='AUTOCOMMIT')
    else:
        engine = sa.create_engine(url)

    if dialect_name == 'sqlite' and database != ':memory:':
        if database:
            os.remove(database)
    elif dialect_name == 'postgresql':
        with engine.begin() as conn:
            # Disconnect all users from the database we are dropping.
            version = conn.dialect.server_version_info
            pid_column = (
                'pid' if (version >= (9, 2)) else 'procpid'
            )
            text = '''
            SELECT pg_terminate_backend(pg_stat_activity.{pid_column})
            FROM pg_stat_activity
            WHERE pg_stat_activity.datname = '{database}'
            AND {pid_column} <> pg_backend_pid();
            '''.format(pid_column=pid_column, database=database)
            conn.execute(sa.text(text))

            # Drop the database.
            text = f'DROP DATABASE {quote(conn, database)}'
            conn.execute(sa.text(text))
    elif dialect_name == 'mssql':
        with engine.begin() as conn:
            print(f"\n{'='*80}\nDIAGNOSTIC INFORMATION BEFORE DROPPING DATABASE '{database}'\n{'='*80}")
            
            # 1. Get active transaction details
            print("\n1. ACTIVE TRANSACTIONS IN DATABASE\n" + "-"*50)
            active_trans_sql = f'''
            SELECT
                at.transaction_id,
                at.name AS transaction_name,
                at.transaction_type,
                at.transaction_state,
                at.transaction_status,
                at.transaction_begin_time,
                stx.session_id,
                s.login_name,
                s.host_name,
                s.program_name,
                r.status as request_status,
                r.command,
                st.text AS sql_text
            FROM sys.dm_tran_active_transactions AS at
            JOIN sys.dm_tran_session_transactions AS stx 
                ON at.transaction_id = stx.transaction_id
            JOIN sys.dm_exec_sessions AS s 
                ON stx.session_id = s.session_id
            LEFT JOIN sys.dm_exec_requests AS r 
                ON s.session_id = r.session_id
            OUTER APPLY sys.dm_exec_sql_text(r.sql_handle) AS st
            WHERE s.database_id = DB_ID('{database}')
            '''
            active_trans_result = conn.execute(sa.text(active_trans_sql))
            rows = active_trans_result.fetchall()
            
            if not rows:
                print("No active transactions found.")
            else:
                for row in rows:
                    session_id = row[6]  # Store for later use in additional queries
                    print(f"Transaction ID      : {row[0]}")
                    print(f"Transaction Name    : {row[1]}")
                    print(f"Transaction Type    : {row[2]}")
                    print(f"Transaction State   : {row[3]}")
                    print(f"Transaction Status  : {row[4]}")
                    print(f"Begin Time          : {row[5]}")
                    print(f"Session ID          : {session_id}")
                    print(f"Login Name          : {row[7]}")
                    print(f"Host Name           : {row[8]}")
                    print(f"Program Name        : {row[9]}")
                    print(f"Request Status      : {row[10]}")
                    print(f"Command             : {row[11]}")
                    print(f"SQL Text            : {row[12]}")
                    print("-" * 50)
                    
                    # 2. For each active transaction, get lock information
                    print(f"\n2. LOCKS HELD BY SESSION {session_id}\n" + "-"*50)
                    locks_sql = f'''
                    SELECT 
                        tl.resource_type,
                        OBJECT_NAME(p.object_id) as object_name,
                        tl.resource_description,
                        tl.request_mode,
                        tl.request_status
                    FROM sys.dm_tran_locks tl
                    LEFT JOIN sys.partitions p ON p.hobt_id = tl.resource_associated_entity_id
                    WHERE tl.request_session_id = {session_id}
                    '''
                    try:
                        locks_result = conn.execute(sa.text(locks_sql))
                        locks = locks_result.fetchall()
                        if not locks:
                            print("No locks found for this session.")
                        else:
                            for lock in locks:
                                print(f"Resource Type      : {lock[0]}")
                                print(f"Object Name        : {lock[1]}")
                                print(f"Resource Description: {lock[2]}")
                                print(f"Request Mode       : {lock[3]}")
                                print(f"Request Status     : {lock[4]}")
                                print("-" * 50)
                    except Exception as e:
                        print(f"Error retrieving locks: {e}")
                    
                    # 3. Get recent SQL statements executed by this session
                    print(f"\n3. RECENT SQL BY SESSION {session_id}\n" + "-"*50)
                    recent_sql = f'''
                    SELECT TOP 5
                        st.text AS sql_text,
                        qs.creation_time,
                        qs.last_execution_time,
                        qs.execution_count
                    FROM sys.dm_exec_query_stats qs
                    CROSS APPLY sys.dm_exec_sql_text(qs.sql_handle) st
                    WHERE qs.plan_handle IN (
                        SELECT plan_handle 
                        FROM sys.dm_exec_sessions s
                        JOIN sys.dm_exec_requests r ON s.session_id = r.session_id
                        WHERE s.session_id = {session_id}
                    )
                    ORDER BY qs.last_execution_time DESC
                    '''
                    try:
                        recent_sql_result = conn.execute(sa.text(recent_sql))
                        recent = recent_sql_result.fetchall()
                        if not recent:
                            print("No recent SQL statements found.")
                        else:
                            for sql in recent:
                                print(f"SQL Text           : {sql[0]}")
                                print(f"Creation Time      : {sql[1]}")
                                print(f"Last Execution     : {sql[2]}")
                                print(f"Execution Count    : {sql[3]}")
                                print("-" * 50)
                    except Exception as e:
                        print(f"Error retrieving recent SQL: {e}")
                    
                    # 4. Check if the transaction is blocking others
                    print(f"\n4. BLOCKING BY SESSION {session_id}\n" + "-"*50)
                    blocking_sql = f'''
                    SELECT 
                        blocked.session_id as blocked_session,
                        blocked.wait_type,
                        blocked.wait_time,
                        st.text as blocked_statement
                    FROM sys.dm_exec_requests blocked
                    CROSS APPLY sys.dm_exec_sql_text(blocked.sql_handle) st
                    WHERE blocked.blocking_session_id = {session_id}
                    '''
                    try:
                        blocking_result = conn.execute(sa.text(blocking_sql))
                        blocking = blocking_result.fetchall()
                        if not blocking:
                            print("This session is not blocking any other sessions.")
                        else:
                            for block in blocking:
                                print(f"Blocked Session    : {block[0]}")
                                print(f"Wait Type          : {block[1]}")
                                print(f"Wait Time (ms)     : {block[2]}")
                                print(f"Blocked Statement  : {block[3]}")
                                print("-" * 50)
                    except Exception as e:
                        print(f"Error retrieving blocking info: {e}")
                
                # 5. Offer to kill blocking sessions
                print("\n5. RECOMMENDATIONS:")
                print("To resolve blocking issues before dropping the database, consider:")
                print("1. Wait for active transactions to complete")
                print("2. Contact application owners to properly close connections")
                print(f"3. Run: KILL <session_id> to terminate specific sessions")
                print("4. Use the WITH ROLLBACK IMMEDIATE option to force termination of connections\n")
                
            # Try to drop the database with safety options
            try:
                print(f"\nATTEMPTING TO DROP DATABASE '{database}'...\n")
                
                # Set the database to single user mode to disconnect all users
                single_user_sql = f'''
                ALTER DATABASE {quote(conn, database)}
                SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
                '''
                conn.execute(sa.text(single_user_sql))
                print(f"Database set to SINGLE_USER mode with ROLLBACK IMMEDIATE.")
                
                # Drop the database
                drop_sql = f'DROP DATABASE {quote(conn, database)}'
                conn.execute(sa.text(drop_sql))
                print(f"Database '{database}' successfully dropped.")
            except Exception as e:
                print(f"Error dropping database: {e}")
                print("\nIf you need to force drop the database, you may need to manually:")
                print("1. Kill all sessions connected to the database")
                print("2. Set the database to single user mode")
                print("3. Drop the database\n")
    else:
        with engine.begin() as conn:
            text = f'DROP DATABASE {quote(conn, database)}'
            conn.execute(sa.text(text))
            
    engine.dispose()
