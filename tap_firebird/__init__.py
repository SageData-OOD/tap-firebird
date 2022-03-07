import copy
import time
from itertools import groupby

import fdb
import datetime
import sys
import simplejson as json

import singer
import singer.metrics as metrics
from singer import metadata
from singer import utils
from singer.catalog import Catalog, CatalogEntry
from singer.schema import Schema

# from tap_Firebird import resolve
import resolve

LOGGER = singer.get_logger()

REQUIRED_CONFIG_KEYS = [
    'host',
    'port',
    'database',
    'user',
    'password',
    'start_date'
]

STRING_TYPES = {'char', 'character', 'nchar', 'bpchar', 'text', 'varchar',
                'character varying', 'nvarchar'}

BYTES_FOR_INTEGER_TYPE = {
    'smallint': 2,
    'integer': 4,
    'int64': 8
}

FLOAT_TYPES = {'float', 'float4', 'float8', 'double', 'd_float'}

DATE_TYPES = {'date'}

DATETIME_TYPES = {'timestamp', 'timestamptz',
                  'timestamp without time zone', 'timestamp with time zone'}

CONFIG = {}


def discover_catalog(conn):
    '''Returns a Catalog describing the structure of the database.'''

    table_spec = select_all(
        conn,
        """
        SELECT RDB$RELATION_NAME as table_name,
        CASE
         WHEN RDB$VIEW_BLR IS NULL THEN 'BASE TABLE'
         ELSE 'VIEW'
        END as table_type
        from RDB$RELATIONS where
        RDB$RELATION_TYPE = 0 and RDB$SYSTEM_FLAG = 0
        """)

    column_specs = select_all(
        conn,
        """
        select rf.RDB$RELATION_NAME AS table_name, rf.RDB$FIELD_POSITION AS ordinal_position, rf.RDB$FIELD_NAME AS column_name,
        CASE F.RDB$FIELD_TYPE
            WHEN 7 THEN 'SMALLINT'
            WHEN 8 THEN 'INTEGER'
            WHEN 9 THEN 'QUAD'
            WHEN 10 THEN 'FLOAT'
            WHEN 11 THEN 'D_FLOAT'
            WHEN 12 THEN 'DATE'
            WHEN 13 THEN 'TIME'
            WHEN 14 THEN 'CHAR'
            WHEN 16 THEN 'INT64'
            WHEN 23 THEN 'BOOLEAN'
            WHEN 27 THEN 'DOUBLE'
            WHEN 35 THEN 'TIMESTAMP'
            WHEN 37 THEN 'VARCHAR'
            WHEN 40 THEN 'CSTRING'
            WHEN 261 THEN 'BLOB'
            ELSE 'UNKNOWN'
        END AS udt_name,
        rf.RDB$NULL_FLAG AS is_nullable
        from rdb$relation_fields rf
        INNER JOIN RDB$RELATIONS r ON r.RDB$RELATION_NAME = rf.rdb$relation_name
        INNER JOIN RDB$FIELDS f ON rf.RDB$FIELD_SOURCE = f.RDB$FIELD_NAME
        where r.RDB$RELATION_TYPE = 0 and r.RDB$SYSTEM_FLAG = 0
        ORDER BY table_name, ordinal_position;
        """)

    """

    """
    pk_specs = select_all(
        conn,
        """
        SELECT
            rc.rdb$relation_name as table_name,
            sg.rdb$field_name as field_name
        from
            rdb$indices ix
            left join rdb$index_segments sg on ix.rdb$index_name = sg.rdb$index_name
            left join rdb$relation_constraints rc on rc.rdb$index_name = ix.rdb$index_name
        where
            rc.rdb$constraint_type = 'PRIMARY KEY'
        ORDER BY
            table_name
        """)

    entries = []
    table_columns = [{'name': k, 'columns': [
        {'pos': t[1], 'name': t[2].strip(), 'type': t[3].strip(),
         'nullable': t[4]} for t in v]}
                     for k, v in groupby(column_specs, key=lambda t: t[0].strip())]

    table_pks = {k.strip(): [t[1].strip() for t in v]
                 for k, v in groupby(pk_specs, key=lambda t: t[0])}

    table_types = dict(table_spec)

    for items in table_columns:
        table_name = items['name']
        qualified_table_name = table_name
        cols = items['columns']
        schema = Schema(type='object',
                        properties={
                            c['name']: schema_for_column(c) for c in cols})
        key_properties = [
            column for column in table_pks.get(table_name, [])
            if schema.properties[column].inclusion != 'unsupported']
        is_view = table_types.get(table_name) == 'VIEW'
        db_name = conn.database_name
        metadata = create_column_metadata(
            db_name, cols, is_view, table_name, key_properties)
        tap_stream_id = qualified_table_name
        entry = CatalogEntry(
            tap_stream_id=tap_stream_id,
            stream=table_name,
            schema=schema,
            table=qualified_table_name,
            metadata=metadata,
            database=db_name
        )

        entries.append(entry)

    return Catalog(entries)


def do_discover(conn):
    LOGGER.info("Running discover")
    discover_catalog(conn).dump()
    LOGGER.info("Completed discover")


def schema_for_column(c):
    '''Returns the Schema object for the given Column.'''
    column_type = c['type'].lower()
    column_nullable = c['nullable']
    inclusion = 'available'
    result = Schema(inclusion=inclusion)

    if column_type == 'boolean':
        result.type = 'boolean'

    elif column_type in BYTES_FOR_INTEGER_TYPE:
        result.type = 'integer'
        bits = BYTES_FOR_INTEGER_TYPE[column_type] * 8
        result.minimum = 0 - 2 ** (bits - 1)
        result.maximum = 2 ** (bits - 1) - 1

    elif column_type in FLOAT_TYPES:
        result.type = 'number'

    elif column_type == 'numeric':
        result.type = 'number'

    elif column_type in STRING_TYPES:
        result.type = 'string'

    elif column_type in DATETIME_TYPES:
        result.type = 'string'
        result.format = 'date-time'

    elif column_type in DATE_TYPES:
        result.type = 'string'
        result.format = 'date'

    else:
        result = Schema(None,
                        inclusion='unsupported',
                        description='Unsupported column type {}'
                        .format(column_type))

    if column_nullable == 1:
        result.type = ['null', result.type]

    return result


def create_column_metadata(
        db_name, cols, is_view,
        table_name, key_properties=[]):
    mdata = metadata.new()
    mdata = metadata.write(mdata, (), 'selected-by-default', False)
    mdata = metadata.write(mdata, (), 'selected', True)
    if not is_view:
        mdata = metadata.write(
            mdata, (), 'table-key-properties', key_properties)
    else:
        mdata = metadata.write(
            mdata, (), 'view-key-properties', key_properties)
    mdata = metadata.write(mdata, (), 'is-view', is_view)
    mdata = metadata.write(mdata, (), 'schema-name', table_name)
    mdata = metadata.write(mdata, (), 'database-name', db_name)
    valid_rep_keys = []

    for c in cols:
        if c['type'].lower() in DATETIME_TYPES:
            valid_rep_keys.append(c['name'])

        schema = schema_for_column(c)

        mdata = metadata.write(mdata,
                               ('properties', c['name']),
                               'selected-by-default',
                               schema.inclusion != 'unsupported')
        mdata = metadata.write(mdata,
                               ('properties', c['name']),
                               'selected',
                               True)
        mdata = metadata.write(mdata,
                               ('properties', c['name']),
                               'sql-datatype',
                               c['type'].lower())
        mdata = metadata.write(mdata,
                               ('properties', c['name']),
                               'inclusion',
                               schema.inclusion)
    if valid_rep_keys:
        mdata = metadata.write(mdata, (), 'valid-replication-keys',
                               valid_rep_keys)
    else:
        mdata = metadata.write(mdata, (), 'forced-replication-method', {
            'replication-method': 'FULL_TABLE',
            'reason': 'No replication keys found from table'})

    return metadata.to_list(mdata)


def open_connection(config):
    cfg = {
        'host': config['host'],
        'database': config['database'],
        'user': config['user'],
        'password': config['password'],
        'port': config['port']
    }
    conn = fdb.connect(**cfg)
    return conn


def select_all(conn, query):
    cur = conn.cursor()
    cur.execute(query)
    column_specs = cur.fetchall()
    cur.close()
    return column_specs


def get_stream_version(tap_stream_id, state):
    return singer.get_bookmark(state,
                               tap_stream_id,
                               "version") or int(time.time() * 1000)


def row_to_record(catalog_entry, version, row, columns, time_extracted):
    row_to_persist = ()

    for idx, elem in enumerate(row):
        if isinstance(elem, datetime.datetime):
            # elem = elem.isoformat('T') + 'Z'
            elem = elem.isoformat('T')
        elif isinstance(elem, datetime.date):
            elem = elem.isoformat()

        row_to_persist += (elem,)
    return singer.RecordMessage(
        stream=catalog_entry.stream,
        record=dict(zip(columns, row_to_persist)),
        version=version,
        time_extracted=time_extracted)


def sync_table(connection, catalog_entry, state):
    columns = list(catalog_entry.schema.properties.keys())
    start_date = CONFIG.get('start_date')
    formatted_start_date = None

    if not columns:
        LOGGER.warning(
            'There are no columns selected for table {}, skipping it'
                .format(catalog_entry.table))
        return

    tap_stream_id = catalog_entry.tap_stream_id
    LOGGER.info('Beginning sync for {} table'.format(tap_stream_id))

    cursor = connection.cursor()
    table = catalog_entry.table
    select = 'SELECT {} FROM {}'.format(
        ','.join('"{}"'.format(c) for c in columns),
        '"{}"'.format(table))

    if start_date is not None:
        formatted_start_date = datetime.datetime.strptime(
            start_date, '%Y-%m-%dT%H:%M:%SZ')

    replication_key = metadata.to_map(catalog_entry.metadata).get(
        (), {}).get('replication-key')
    replication_key_value = None
    bookmark_is_empty = state.get('bookmarks', {}).get(
        tap_stream_id) is None
    stream_version = get_stream_version(tap_stream_id, state)
    state = singer.write_bookmark(
        state,
        tap_stream_id,
        'version',
        stream_version
    )
    activate_version_message = singer.ActivateVersionMessage(
        stream=catalog_entry.stream,
        version=stream_version
    )

    # If there's a replication key, we want to emit an ACTIVATE_VERSION
    # message at the beginning so the records show up right away. If
    # there's no bookmark at all for this stream, assume it's the very
    # first replication. That is, clients have never seen rows for this
    # stream before, so they can immediately acknowledge the present
    # version.
    if replication_key or bookmark_is_empty:
        yield activate_version_message

    if replication_key:
        replication_key_value = singer.get_bookmark(
            state,
            tap_stream_id,
            'replication_key_value'
        ) or str(formatted_start_date)

    if replication_key_value is not None:
        try:
            replication_key_value = datetime.datetime.strptime(replication_key_value, '%Y-%m-%dT%H:%M:%S.%f')\
                .strftime("%Y-%m-%d %H:%M:%S")
        except Exception:
            pass

        select += ' WHERE {} >= \'{}\' ORDER BY {} ' \
                  'ASC'.format(replication_key, replication_key_value, replication_key)

    elif replication_key is not None:
        select += ' ORDER BY {} ASC'.format(replication_key)

    time_extracted = utils.now()
    LOGGER.info('Running {}'.format(select))
    cursor.execute(select)
    row = cursor.fetchone()
    rows_saved = 0

    with metrics.record_counter(None) as counter:
        counter.tags['database'] = catalog_entry.database
        counter.tags['table'] = catalog_entry.table
        while row:
            counter.increment()
            rows_saved += 1
            record_message = row_to_record(catalog_entry,
                                           stream_version,
                                           row,
                                           columns,
                                           time_extracted)
            yield record_message

            if replication_key is not None:
                state = singer.write_bookmark(state,
                                              tap_stream_id,
                                              'replication_key_value',
                                              record_message.record[
                                                  replication_key])
            if rows_saved % 1000 == 0:
                yield singer.StateMessage(value=copy.deepcopy(state))
            row = cursor.fetchone()

    if not replication_key:
        yield activate_version_message
        state = singer.write_bookmark(state, catalog_entry.tap_stream_id,
                                      'version', None)

    yield singer.StateMessage(value=copy.deepcopy(state))


def generate_messages(conn, catalog, state):
    catalog = resolve.resolve_catalog(discover_catalog(conn),
                                      catalog, state)

    for catalog_entry in catalog.streams:
        state = singer.set_currently_syncing(state,
                                             catalog_entry.tap_stream_id)
        catalog_md = metadata.to_map(catalog_entry.metadata)

        if catalog_md.get((), {}).get('is-view'):
            key_properties = catalog_md.get((), {}).get('view-key-properties')
        else:
            key_properties = catalog_md.get((), {}).get('table-key-properties')
        bookmark_properties = catalog_md.get((), {}).get('replication-key')

        # Emit a state message to indicate that we've started this stream
        yield singer.StateMessage(value=copy.deepcopy(state))

        # Emit a SCHEMA message before we sync any records
        yield singer.SchemaMessage(
            stream=catalog_entry.stream,
            schema=catalog_entry.schema.to_dict(),
            key_properties=key_properties,
            bookmark_properties=bookmark_properties)

        # Emit a RECORD message for each record in the result set
        with metrics.job_timer('sync_table') as timer:
            timer.tags['database'] = catalog_entry.database
            timer.tags['table'] = catalog_entry.table
            for message in sync_table(conn, catalog_entry, state):
                yield message

    # If we get here, we've finished processing all the streams, so clear
    # currently_syncing from the state and emit a state message.
    state = singer.set_currently_syncing(state, None)
    yield singer.StateMessage(value=copy.deepcopy(state))


def coerce_datetime(o):
    if isinstance(o, (datetime.datetime, datetime.date)):
        return o.isoformat()
    raise TypeError("Type {} is not serializable".format(type(o)))


def do_sync(conn, catalog, state):
    LOGGER.info("Starting Firebird sync")
    for message in generate_messages(conn, catalog, state):
        sys.stdout.write(json.dumps(message.asdict(),
                                    default=coerce_datetime,
                                    use_decimal=True) + '\n')
        sys.stdout.flush()
    LOGGER.info("Completed sync")


def build_state(raw_state, catalog):
    LOGGER.info('Building State from raw state {}'.format(raw_state))

    state = {}

    currently_syncing = singer.get_currently_syncing(raw_state)
    if currently_syncing:
        state = singer.set_currently_syncing(state, currently_syncing)

    for catalog_entry in catalog.streams:
        tap_stream_id = catalog_entry.tap_stream_id
        catalog_metadata = metadata.to_map(catalog_entry.metadata)
        replication_method = catalog_metadata.get(
            (), {}).get('replication-method')
        raw_stream_version = singer.get_bookmark(
            raw_state, tap_stream_id, 'version')

        if replication_method == 'INCREMENTAL':
            replication_key = catalog_metadata.get(
                (), {}).get('replication-key')

            state = singer.write_bookmark(
                state, tap_stream_id, 'replication_key', replication_key)

            # Only keep the existing replication_key_value if the
            # replication_key hasn't changed.
            raw_replication_key = singer.get_bookmark(raw_state,
                                                      tap_stream_id,
                                                      'replication_key')
            if raw_replication_key == replication_key:
                raw_replication_key_value = singer.get_bookmark(
                    raw_state, tap_stream_id, 'replication_key_value')
                state = singer.write_bookmark(state,
                                              tap_stream_id,
                                              'replication_key_value',
                                              raw_replication_key_value)

            if raw_stream_version is not None:
                state = singer.write_bookmark(
                    state, tap_stream_id, 'version', raw_stream_version)

        elif replication_method == 'FULL_TABLE' and raw_stream_version is None:
            state = singer.write_bookmark(state,
                                          tap_stream_id,
                                          'version',
                                          raw_stream_version)

    return state


def main_impl():
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)
    CONFIG.update(args.config)
    connection = open_connection(args.config)

    if args.discover:
        do_discover(connection)
    elif args.catalog:
        state = build_state(args.state, args.catalog)
        do_sync(connection, args.catalog, state)
    elif args.properties:
        catalog = Catalog.from_dict(args.properties)
        state = build_state(args.state, catalog)
        do_sync(connection, catalog, state)
    else:
        LOGGER.info("No properties were selected")

    connection.close()


@utils.handle_top_exception(LOGGER)
def main():
    main_impl()


if __name__ == '__main__':
    main()
