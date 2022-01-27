#!/usr/bin/env python3
import os
import json
import backoff
import requests
import singer
from datetime import datetime, timedelta
from singer import utils, metadata
from singer.catalog import Catalog, CatalogEntry
from singer.schema import Schema
from singer.transform import transform

REQUIRED_CONFIG_KEYS = ["start_date", "guild_id", "bot_token", "refresh_token", "client_id", "client_secret"]
LOGGER = singer.get_logger()
HOST = "https://discord.com"
PATH = "/api/v8"
END_POINTS = {
    "refresh_token": "/oauth2/token",
    "user_guild": "/guilds/{guild_id}?with_counts=true",
    "guild_channels": "/guilds/{guild_id}/channels",
    "guild_members": "/guilds/{guild_id}/members",
    "channels_messages": "/channels/{channel_id}/messages"
}

FULL_TABLE_SYNC_STREAMS = ["user_guild", "guild_channels"]
INCREMENTAL_SYNC_STREAMS = ["guild_members", "channels_messages"]

# Milliseconds since Discord Epoch, the first second of 2015 -> 1420070400000.
# More details: https://discord.com/developers/docs/reference#snowflakes
DISCORD_EPOCH = 1420070400000


class DiscordRateLimitError(Exception):
    def __init__(self, msg):
        self.msg = msg
        super().__init__(self.msg)


def snowflake_time(id):
    """ Discord snowflake ID to datetime(str) conversion
        More details: https://discord.com/developers/docs/reference#snowflakes
    """
    return str(datetime.utcfromtimestamp(((id >> 22) + DISCORD_EPOCH) / 1000))


def time_snowflake(datetime_obj, high=False):
    """Returns a numeric snowflake pretending to be created at the given date.
       More details: https://discord.com/developers/docs/reference#snowflakes
    """

    unix_seconds = (datetime_obj - type(datetime_obj)(1970, 1, 1)).total_seconds()
    discord_millis = int(unix_seconds * 1000 - DISCORD_EPOCH)

    snowflake =  (discord_millis << 22) + (2**22-1 if high else 0)
    return snowflake if snowflake >= 0 else 0


def _refresh_token(config):
    headers = {'Content-Type': 'application/x-www-form-urlencoded'}
    data = {
        'grant_type': 'refresh_token',
        'refresh_token': config['refresh_token']
    }
    url = HOST + PATH + END_POINTS["refresh_token"]
    response = requests.post(url, headers=headers, data=data,
                             auth=(config["client_id"], config['client_secret']))
    return response.json()


def refresh_access_token_if_expired(config):
    # if [expires_at not exist] or if [exist and less than current time] then it will update the token
    if config.get('expires_at') is None or config.get('expires_at') < datetime.utcnow():
        res = _refresh_token(config)
        config["access_token"] = res["access_token"]
        config["refresh_token"] = res["refresh_token"]
        config["expires_at"] = datetime.utcnow() + timedelta(seconds=res["expires_in"])
        return True
    return False


def get_key_properties(stream_id):
    return ["id"]


def get_bookmark(stream_id):
    return "id"


def get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)


def load_schemas():
    """ Load schemas from schemas folder """
    schemas = {}
    for filename in os.listdir(get_abs_path('schemas')):
        path = get_abs_path('schemas') + '/' + filename
        file_raw = filename.replace('.json', '')
        with open(path) as file:
            schemas[file_raw] = Schema.from_dict(json.load(file))
    return schemas


def create_metadata_for_report(stream_id, schema, key_properties):
    replication_key = get_bookmark(stream_id)
    mdata = [{"breadcrumb": [], "metadata": {"inclusion": "available", "forced-replication-method": "FULL_TABLE"}}]

    if key_properties:
        mdata[0]["metadata"]["table-key-properties"] = key_properties

    if stream_id in INCREMENTAL_SYNC_STREAMS:
        mdata[0]["metadata"]["forced-replication-method"] = "INCREMENTAL"
        mdata[0]["metadata"]["valid-replication-keys"] = [replication_key]

    for key in schema.properties:
        # hence, when property is object, we will only consider properties of that object without taking object itself.
        if "object" in schema.properties.get(key).type and schema.properties.get(key).properties:
            inclusion = "available"
            mdata.extend(
                [{"breadcrumb": ["properties", key, "properties", prop], "metadata": {"inclusion": inclusion}} for prop
                 in schema.properties.get(key).properties])
        else:
            inclusion = "automatic" if key in key_properties else "available"
            mdata.append({"breadcrumb": ["properties", key], "metadata": {"inclusion": inclusion}})

    return mdata


def discover():
    raw_schemas = load_schemas()
    streams = []
    for stream_id, schema in raw_schemas.items():
        stream_metadata = create_metadata_for_report(stream_id, schema, get_key_properties(stream_id))
        key_properties = get_key_properties(stream_id)
        streams.append(
            CatalogEntry(
                tap_stream_id=stream_id,
                stream=stream_id,
                schema=schema,
                key_properties=key_properties,
                metadata=stream_metadata
            )
        )
    return Catalog(streams)


@backoff.on_exception(backoff.expo, DiscordRateLimitError, max_tries=5, factor=2)
@utils.ratelimit(1, 1)
def request_data(config, attr, headers, endpoint, channel_id=None):
    url = HOST + PATH + endpoint.format(guild_id=config["guild_id"], channel_id=channel_id)
    if attr:
        url += "?" + "&".join([f"{k}={v}" for k, v in attr.items()])

    # # We are not using access_token for now, if we want to use access_token then also we have to print_metrics
    # # in order to save new refresh_token to S3, after refreshing the access_token each time.
    # if refresh_access_token_if_expired(config) or not headers:
    #     headers.update({'Authorization': f'Bearer {config["access_token"]}'})

    if not headers:
        headers.update({'Authorization': f'Bot {config["bot_token"]}'})

    response = requests.get(url, headers=headers)
    if response.status_code == 429:
        raise DiscordRateLimitError(response.text)
    elif response.status_code != 200:
        raise Exception(response.text)
    data = response.json()
    return [data] if isinstance(data, dict) else data


def get_list_channel_ids(config):
    endpoint = END_POINTS["guild_channels"]
    channels = request_data(config, {}, {}, endpoint)
    return [c["id"] for c in channels]


def sync_incremental(config, state, stream):
    bookmark_column = get_bookmark(stream.tap_stream_id)
    mdata = metadata.to_map(stream.metadata)
    schema = stream.schema.to_dict()

    singer.write_schema(
        stream_name=stream.tap_stream_id,
        schema=schema,
        key_properties=stream.key_properties,
    )
    endpoint = END_POINTS[stream.tap_stream_id]
    headers = dict()
    attr = {"limit": 100} if stream.tap_stream_id == "guild_members" else {}
    start_id = singer.get_bookmark(state, stream.tap_stream_id, bookmark_column) \
        if state.get("bookmarks", {}).get(stream.tap_stream_id) \
        else time_snowflake(datetime.strptime(config["start_date"], "%Y-%m-%d"))

    if stream.tap_stream_id == "channels_messages":
        channel_ids = get_list_channel_ids(config)
        current_datetime_before_fetch_messages = datetime.utcnow()
        for cid in channel_ids:
            bookmark = start_id
            while True:
                attr["after"] = bookmark
                LOGGER.info("Channel_id --> %s :: Querying Date --> %s", cid, snowflake_time(bookmark))
                tap_data = request_data(config, attr, headers, endpoint, channel_id=cid)

                with singer.metrics.record_counter(stream.tap_stream_id) as counter:
                    for row in tap_data:
                        # Type Conversation and Transformation
                        transformed_data = transform(row, schema, metadata=mdata)

                        # write one or more rows to the stream:
                        singer.write_records(stream.tap_stream_id, [transformed_data])
                        counter.increment()
                        bookmark = max([bookmark, int(row[bookmark_column])])
                if not tap_data:
                    break

        # Write state after fetching all the messages among all the channels.
        bookmark = time_snowflake(current_datetime_before_fetch_messages)
        state = singer.write_bookmark(state, stream.tap_stream_id, bookmark_column, bookmark)
        singer.write_state(state)
    else:
        bookmark = start_id
        while True:
            attr["after"] = bookmark
            LOGGER.info("Querying Date --> %s", snowflake_time(bookmark))
            tap_data = request_data(config, attr, headers, endpoint)

            with singer.metrics.record_counter(stream.tap_stream_id) as counter:
                for row in tap_data:
                    # Type Conversation and Transformation
                    if stream.tap_stream_id == "guild_members":
                        row["id"] = row.get("user", {})["id"]
                    transformed_data = transform(row, schema, metadata=mdata)

                    # write one or more rows to the stream:
                    singer.write_records(stream.tap_stream_id, [transformed_data])
                    counter.increment()
                    bookmark = max([bookmark, int(row[bookmark_column])])

            # if there is data, then only we will print state
            if tap_data:
                state = singer.write_bookmark(state, stream.tap_stream_id, bookmark_column, bookmark)
                singer.write_state(state)
            else:
                # if no data available
                break


def sync_full_table(config, state, stream):
    mdata = metadata.to_map(stream.metadata)
    schema = stream.schema.to_dict()

    singer.write_schema(
        stream_name=stream.tap_stream_id,
        schema=schema,
        key_properties=stream.key_properties,
    )
    endpoint = END_POINTS[stream.tap_stream_id]
    tap_data = request_data(config, {}, {}, endpoint)

    with singer.metrics.record_counter(stream.tap_stream_id) as counter:
        for row in tap_data:
            # Type Conversation and Transformation
            transformed_data = transform(row, schema, metadata=mdata)

            # write one or more rows to the stream:
            singer.write_records(stream.tap_stream_id, [transformed_data])
            counter.increment()


def sync(config, state, catalog):
    # Loop over selected streams in catalog
    for stream in catalog.get_selected_streams(state):
        LOGGER.info("Syncing stream:" + stream.tap_stream_id)

        if stream.tap_stream_id in INCREMENTAL_SYNC_STREAMS:
            sync_incremental(config, state, stream)
        else:
            sync_full_table(config, state, stream)
    return


@utils.handle_top_exception(LOGGER)
def main():
    # Parse command line arguments
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)

    if args.discover:
        catalog = discover()
        catalog.dump()
    else:
        if args.catalog:
            catalog = args.catalog
        else:
            catalog = discover()
        sync(args.config, args.state, catalog)


if __name__ == "__main__":
    main()


