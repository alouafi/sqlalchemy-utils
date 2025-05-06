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
            # Print detailed information about active transactions in the database
            sql = f'''
            SELECT
                at.transaction_id,
                at.name AS transaction_name,
                at.transaction_type,
                at.transaction_state,
                at.transaction_status,
                at.transaction_begin_time,
                s.session_id,
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
        print(f"\nActive transactions in database '{database}' before dropping:\n{'='*60}")
        result = conn.execute(sa.text(sql))
        for row in result:
            print(f"Transaction ID      : {row['transaction_id']}")
            print(f"Transaction Name    : {row['transaction_name']}")
            print(f"Transaction Type    : {row['transaction_type']}")
            print(f"Transaction State   : {row['transaction_state']}")
            print(f"Transaction Status  : {row['transaction_status']}")
            print(f"Begin Time          : {row['transaction_begin_time']}")
            print(f"Session ID          : {row['session_id']}")
            print(f"Login Name          : {row['login_name']}")
            print(f"Host Name           : {row['host_name']}")
            print(f"Program Name        : {row['program_name']}")
            print(f"Request Status      : {row['request_status']}")
            print(f"Command             : {row['command']}")
            print(f"SQL Text            : {row['sql_text']}")
            print("-" * 60)


            # Set the database to single user mode to disconnect all users
            #text = f'''
            #ALTER DATABASE {quote(conn, database)}
            #SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
            #'''
            #conn.execute(sa.text(text))

            # Drop the database
            text = f'DROP DATABASE {quote(conn, database)}'
            conn.execute(sa.text(text))
    else:
        with engine.begin() as conn:
            text = f'DROP DATABASE {quote(conn, database)}'
            conn.execute(sa.text(text))
