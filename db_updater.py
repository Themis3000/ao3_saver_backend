CURRENT_VERSION = 2


def get_db_version(conn):
    cursor = conn.cursor()
    cursor.execute(
        "select exists(select * from information_schema.tables where table_name='queue')")
    has_queue = cursor.fetchone()[0]

    if not has_queue:
        cursor.close()
        return 0

    cursor.execute("select exists(select * from information_schema.tables where table_name='version_info')")
    has_version = cursor.fetchone()[0]
    if not has_version:
        cursor.close()
        return 1

    cursor.execute("select version from version_info")
    version = cursor.fetchone()[0]
    cursor.close()
    if not version:
        raise Exception("Unable to read database version properly")
    return version


def ensure_schema_updated(conn):
    version = get_db_version(conn)
    print(f"got db version {version}")

    if version == CURRENT_VERSION:
        return

    init_cursor = conn.cursor()
    if version == 0:  # Migration script for version 0 -> 1
        with open("db_init.sql", "r") as f:
            init_cursor.execute(f.read())
    elif version == 1:  # Migration script for version 1 -> 2
        init_cursor.execute("""
            create table unfetched_objects
            (
                object_id       integer default nextval('object_index_object_id_seq'::regclass) not null
                    constraint unfetched_objects_pk
                        primary key,
                request_url     varchar(2000)                                                   not null,
                associated_work integer                                                         not null,
                stalled         boolean default false                                           not null
            );
            
            create table object_dispatches
            (
                dispatch_id        serial                                       not null
                    constraint object_dispatches_pk
                        primary key,
                dispatched_time    TIMESTAMP(0) WITHOUT TIME ZONE default NOW() not null,
                dispatched_to_name varchar(255)                                 not null,
                object_id          integer                                      not null,
                fail_reported      boolean                        default false not null,
                complete           boolean                        default false not null
            );
            
            create table version_info
            (
                version integer not null
            );
            
            INSERT INTO public.version_info (version)
            VALUES (2);
        """)

    init_cursor.close()
    conn.commit()
    ensure_schema_updated(conn)
