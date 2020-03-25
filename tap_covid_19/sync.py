
import base64
import io
import csv
import singer
from singer import metrics, metadata, Transformer, utils
from singer.utils import strptime_to_utc
from tap_covid_19.streams import STREAMS
from tap_covid_19.transform import transform_record

LOGGER = singer.get_logger()


def write_schema(catalog, stream_name):
    stream = catalog.get_stream(stream_name)
    schema = stream.schema.to_dict()
    try:
        singer.write_schema(stream_name, schema, stream.key_properties)
    except OSError as err:
        LOGGER.info('OS Error writing schema for: {}'.format(stream_name))
        raise err


def write_record(stream_name, record, time_extracted):
    try:
        singer.messages.write_record(stream_name, record, time_extracted=time_extracted)
    except OSError as err:
        LOGGER.info('OS Error writing record for: {}'.format(stream_name))
        LOGGER.info('record: {}'.format(record))
        raise err


def get_bookmark(state, stream, default):
    if (state is None) or ('bookmarks' not in state):
        return default
    return (
        state
        .get('bookmarks', {})
        .get(stream, default)
    )


def write_bookmark(state, stream, value):
    if 'bookmarks' not in state:
        state['bookmarks'] = {}
    state['bookmarks'][stream] = value
    LOGGER.info('Write state for stream: {}, value: {}'.format(stream, value))
    singer.write_state(state)


def transform_datetime(this_dttm):
    with Transformer() as transformer:
        new_dttm = transformer._transform_datetime(this_dttm)
    return new_dttm


def process_records(catalog, #pylint: disable=too-many-branches
                    stream_name,
                    records,
                    time_extracted,
                    bookmark_field=None,
                    max_bookmark_value=None,
                    last_datetime=None):
    stream = catalog.get_stream(stream_name)
    schema = stream.schema.to_dict()
    stream_metadata = metadata.to_map(stream.metadata)

    with metrics.record_counter(stream_name) as counter:
        for record in records:
            # Transform record for Singer.io
            with Transformer() as transformer:
                transformed_record = transformer.transform(
                    record, schema, stream_metadata)

                # LOGGER.info('transformed_record: {}'.format(transformed_record)) # COMMENT OUT
                if bookmark_field and (bookmark_field in transformed_record):
                    last_dttm = transform_datetime(last_datetime)
                    bookmark_dttm = transform_datetime(transformed_record[bookmark_field])
                    max_bookmark_dttm = transform_datetime(max_bookmark_value)
                    # Reset max_bookmark_value to new value if higher
                    if (max_bookmark_value is None) or (bookmark_dttm > max_bookmark_dttm):
                        max_bookmark_value = transformed_record[bookmark_field]
                    # Keep only records whose bookmark is after the last_datetime
                    if bookmark_dttm >= last_dttm:
                        write_record(stream_name, transformed_record, time_extracted=time_extracted)
                        counter.increment()
                else:
                    write_record(stream_name, transformed_record, time_extracted=time_extracted)
                    counter.increment()

        return max_bookmark_value, counter.value


# Sync a specific endpoint.
def sync_endpoint(client, #pylint: disable=too-many-branches
                  catalog,
                  state,
                  start_date,
                  stream_name,
                  search_path,
                  endpoint_config,
                  bookmark_field=None,
                  selected_streams=None):

    # Endpoint parameters
    bookmark_query_field = endpoint_config.get('bookmark_query_field', None)
    data_key = endpoint_config.get('data_key', stream_name)
    LOGGER.info('data_key = {}'.format(data_key))

    # Get the latest bookmark for the stream and set the last_datetime
    last_datetime = get_bookmark(state, stream_name, start_date)
    file_max_bookmark_value = last_datetime

    # Convert to GitHub date format, example: Sun, 13 Oct 2019 22:40:01 GMT
    last_dttm = strptime_to_utc(last_datetime)
    last_modified = last_dttm.strftime("%a, %d %b %Y %H:%M:%S %Z'")
    LOGGER.info('HEADER If-Modified-Since: {}'.format(last_modified))

    # Write schema and log selected fields for file stream and child csv stream(s)
    write_schema(catalog, stream_name)
    selected_fields = get_selected_fields(catalog, stream_name)
    LOGGER.info('Stream: {}, selected_fields: {}'.format(stream_name, selected_fields))
    children = endpoint_config.get('children')
    if children:
        for child_stream_name, child_endpoint_config in children.items():
            if child_stream_name in selected_streams:
                write_schema(catalog, child_stream_name)
                child_selected_fields = get_selected_fields(catalog, child_stream_name)
                LOGGER.info('Stream: {}, selected_fields: {}'.format(
                    child_stream_name, child_selected_fields))

    # pagination: loop thru all pages of data using next_url (if not None)
    page = 1
    offset = 0
    file_total_records = 0
    csv_total_records = 0
    next_url = '{}/{}'.format(client.base_url, search_path)

    i = 1
    while next_url is not None:
        LOGGER.info('Search URL for Stream {}: {}'.format(stream_name, next_url))

        # API request search_data
        search_data = {}
        search_data, next_url = client.get(
            url=next_url,
            endpoint=stream_name)
        LOGGER.info('next_url = {}'.format(next_url))
        # LOGGER.info('search_data = {}'.format(search_data)) # COMMENT OUT

        # time_extracted: datetime when the data was extracted from the API
        time_extracted = utils.now()
        search_items = search_data.get(data_key, [])
        if not search_items:
            LOGGER.info('Stream: {}, no files found'.format(stream_name))
            break # No data results

        file_count = 0
        file_records = []
        csv_records = []
        for item in search_items:
            file_count = file_count + 1
            file_url = item.get('url')
            LOGGER.info('File URL for Stream {}: {}'.format(stream_name, file_url))
            file_data = {}
            headers = {}
            if bookmark_query_field:
                headers[bookmark_query_field] = last_modified
            # API request file_data for item, single-file (ignore file_next_url)
            file_data, file_next_url = client.get(
                url=file_url,
                headers=headers,
                endpoint=stream_name)
            # LOGGER.info('file_data: {}'.format(file_data)) # TESTING ONLY - COMMENT OUT

            if file_data:
                content = file_data.get('content')
                content_list = []
                if content:
                    content_b64 = base64.b64decode(content)
                    content_str = content_b64.decode('utf-8')
                    file_like_object = io.StringIO(content_str)
                    
                    with file_like_object as f:
                        reader = csv.DictReader(f)
                        content_list = [r for r in reader]

                file_modified = file_data.get('last_modified')
                file_sha = file_data.get('sha')
                file_path = file_data.get('path')
                file_name = file_data.get('name')

                # Remove _links, content nodes
                file_data.pop('_links', None)
                file_data.pop('content', None)

                # LOGGER.info('file_data: {}'.format(file_data)) # TESTING ONLY - COMMENT OUT
                file_records.append(file_data)

                # Loop thru each child object and append csv records
                if children:
                    for child_stream_name, child_endpoint_config in children.items():
                        if child_stream_name in selected_streams:
                            i = 1
                            for record in content_list:
                                record['git_path'] = file_path
                                record['git_sha'] = file_sha
                                record['git_last_modified'] = file_modified
                                record['git_file_name'] = file_name
                                record['row_number'] = i

                                # Transform record and append
                                transformed_csv_record = {}
                                transformed_csv_record = transform_record(child_stream_name, record)
                                csv_records.append(transformed_csv_record)

                                i = i + 1

        # Process file_records and get the max_bookmark_value and record_count
        file_max_bookmark_value, file_record_count = process_records(
            catalog=catalog,
            stream_name=stream_name,
            records=file_records,
            time_extracted=time_extracted,
            bookmark_field=bookmark_field,
            max_bookmark_value=file_max_bookmark_value,
            last_datetime=last_datetime)
        LOGGER.info('Stream {}, batch processed {} records'.format(
            stream_name, file_record_count))
        file_total_records = file_total_records + file_record_count

        # Loop thru each child object to process csv records
        if children:
            for child_stream_name, child_endpoint_config in children.items():
                if child_stream_name in selected_streams:
                    csv_max_bookmark_value, csv_record_count = process_records(
                        catalog=catalog,
                        stream_name=child_stream_name,
                        records=csv_records,
                        time_extracted=time_extracted,
                        bookmark_field=None,
                        max_bookmark_value=None,
                        last_datetime=last_datetime)
                    LOGGER.info('Stream {}, batch processed {} records'.format(
                        child_stream_name, csv_record_count))
                    csv_total_records = csv_total_records + csv_record_count

        # to_rec: to record; ending record for the batch page
        to_rec = offset + file_count
        LOGGER.info('Synced Stream: {}, page: {}, records: {} to {}'.format(
            stream_name,
            page,
            offset,
            to_rec))
        # Pagination: increment the offset by the limit (batch-size) and page
        offset = offset + file_count
        page = page + 1
        i = i + 1

    # Update the state with the max_bookmark_value for the stream
    if bookmark_field:
        write_bookmark(state, stream_name, file_max_bookmark_value)

    # Return total_records across all pages
    LOGGER.info('Synced Stream: {}, TOTAL pages: {}, file records: {}, csv records: {}'.format(
        stream_name,
        page - 1,
        file_total_records,
        csv_total_records))
    return file_total_records


# Currently syncing sets the stream currently being delivered in the state.
# If the integration is interrupted, this state property is used to identify
#  the starting point to continue from.
# Reference: https://github.com/singer-io/singer-python/blob/master/singer/bookmarks.py#L41-L46
def update_currently_syncing(state, stream_name):
    if (stream_name is None) and ('currently_syncing' in state):
        del state['currently_syncing']
    else:
        singer.set_currently_syncing(state, stream_name)
    singer.write_state(state)


# List selected fields from stream catalog
def get_selected_fields(catalog, stream_name):
    stream = catalog.get_stream(stream_name)
    mdata = metadata.to_map(stream.metadata)
    mdata_list = singer.metadata.to_list(mdata)
    selected_fields = []
    for entry in mdata_list:
        field = None
        try:
            field = entry['breadcrumb'][1]
            if entry.get('metadata', {}).get('selected', False):
                selected_fields.append(field)
        except IndexError:
            pass
    return selected_fields

def sync(client, config, catalog, state):
    start_date = config.get('start_date')

    # Get selected_streams from catalog, based on state last_stream
    #   last_stream = Previous currently synced stream, if the load was interrupted
    last_stream = singer.get_currently_syncing(state)
    LOGGER.info('last/currently syncing stream: {}'.format(last_stream))
    selected_streams = []
    for stream in catalog.get_selected_streams(state):
        selected_streams.append(stream.stream)
    LOGGER.info('selected_streams: {}'.format(selected_streams))

    if not selected_streams:
        return

    # Loop through selected_streams
    for stream_name, endpoint_config in STREAMS.items():
        if stream_name in selected_streams:
            LOGGER.info('START Syncing Stream: {}'.format(stream_name))
            update_currently_syncing(state, stream_name)
            search_path = endpoint_config.get('search_path', stream_name)
            bookmark_field = next(iter(endpoint_config.get('replication_keys', [])), None)
            total_records = sync_endpoint(
                client=client,
                catalog=catalog,
                state=state,
                start_date=start_date,
                stream_name=stream_name,
                search_path=search_path,
                endpoint_config=endpoint_config,
                bookmark_field=bookmark_field,
                selected_streams=selected_streams)

            update_currently_syncing(state, None)
            LOGGER.info('FINISHED Syncing Stream: {}, total_records: {}'.format(
                stream_name,
                total_records))
