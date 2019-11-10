from copy import deepcopy
import uuid

import arrow
from jsonschema import Draft4Validator, FormatChecker
from jsonschema.exceptions import ValidationError
import singer

from target_postgres import json_schema
from target_postgres.exceptions import SingerStreamError
from target_postgres.pysize import get_size

LOGGER = singer.get_LOGGER()


SINGER_RECEIVED_AT = '_sdc_received_at'
SINGER_BATCHED_AT = '_sdc_batched_at'
SINGER_SEQUENCE = '_sdc_sequence'
SINGER_TABLE_VERSION = '_sdc_table_version'
SINGER_PK = '_sdc_primary_key'
SINGER_SOURCE_PK_PREFIX = '_sdc_source_key_'
SINGER_LEVEL = '_sdc_level_{}_id'
SINGER_VALUE = '_sdc_value'


class BufferedSingerStream():
    def __init__(self,
                 stream,
                 schema,
                 key_properties,
                 *args,
                 invalid_records_detect=None,
                 invalid_records_threshold=None,
                 max_rows=200000,
                 max_buffer_size=104857600,  # 100MB
                 **kwargs):
        """
        :param invalid_records_detect: Defaults to True when value is None
        :param invalid_records_threshold: Defaults to 0 when value is None
        """
        self.schema = None
        self.key_properties = None
        self.validator = None
        self.update_schema(schema, key_properties)

        self.stream = stream
        self.invalid_records = []
        self.max_rows = max_rows
        self.max_buffer_size = max_buffer_size

        self.invalid_records_detect = invalid_records_detect
        self.invalid_records_threshold = invalid_records_threshold

        if self.invalid_records_detect is None:
            self.invalid_records_detect = True
        if self.invalid_records_threshold is None:
            self.invalid_records_threshold = 0

        self.__buffer = []
        self.__count = 0
        self.__size = 0
        self.__lifetime_max_version = None

        self.__debug_reporting_interval = int(self.max_rows / 10)

        LOGGER.debug('Stream `{}` created. `max_rows`: {} `max_buffer_size`: {}'.format(
            self.stream,
            self.max_rows,
            self.max_buffer_size
        ))

    def update_schema(self, schema, key_properties):
        # In order to determine whether a value _is in_ properties _or not_ we need to flatten `$ref`s etc.
        self.schema = json_schema.simplify(schema)
        self.key_properties = deepcopy(key_properties)
        self.validator = Draft4Validator(self.schema, format_checker=FormatChecker())

        properties = self.schema['properties']

        if SINGER_RECEIVED_AT not in properties:
            properties[SINGER_RECEIVED_AT] = {
                'type': ['null', 'string'],
                'format': 'date-time'
            }

        if SINGER_SEQUENCE not in properties:
            properties[SINGER_SEQUENCE] = {
                'type': ['null', 'integer']
            }

        if SINGER_TABLE_VERSION not in properties:
            properties[SINGER_TABLE_VERSION] = {
                'type': ['null', 'integer']
            }

        if SINGER_BATCHED_AT not in properties:
            properties[SINGER_BATCHED_AT] = {
                'type': ['null', 'string'],
                'format': 'date-time'
            }

        if len(self.key_properties) == 0:
            self.use_uuid_pk = True
            self.key_properties = [SINGER_PK]
            properties[SINGER_PK] = {
                'type': ['string']
            }
        else:
            self.use_uuid_pk = False

    @property
    def count(self):
        return self.__count

    @property
    def buffer_full(self):
        if self.__count >= self.max_rows:
            LOGGER.debug('Stream `{}` cutting batch due to row count being {:.2%} {}/{}'.format(
                self.stream,
                self.__count / self.max_rows,
                self.__count,
                self.max_rows
            ))
            return True

        if self.__count > 0:
            if self.__size >= self.max_buffer_size:
                LOGGER.debug('Stream `{}` cutting batch due to bytes being {:.2%} {}/{}'.format(
                    self.stream,
                    self.__size / self.max_buffer_size,
                    self.__size,
                    self.max_buffer_size
                ))
                return True

        return False

    @property
    def max_version(self):
        return self.__lifetime_max_version

    def __update_version(self, version):
        if version is None or (self.__lifetime_max_version is not None and self.__lifetime_max_version >= version):
            return None

        if self.__count:
            LOGGER.debug('WARNING: Stream `{}` dropping {} records due to version being updated from: `{}` to: `{}`'.format(
                self.stream,
                self.__count,
                self.__lifetime_max_version,
                version
            ))

        self.flush_buffer()
        self.__lifetime_max_version = version

    def _debug_report_on_buffer_sizes(self):
        if self.__count % self.__debug_reporting_interval == 0:
            LOGGER.debug('Stream `{}` has {:.2%} {}/{} rows filled'.format(
                self.stream,
                self.__count / self.max_rows,
                self.__count,
                self.max_rows
            ))
            LOGGER.debug('Stream `{}` has {:.2%} {}/{} bytes filled'.format(
                self.stream,
                self.__size / self.max_buffer_size,
                self.__size,
                self.max_buffer_size
            ))

    def add_record_message(self, record_message):
        add_record = True

        self.__update_version(record_message.get('version'))

        if self.__lifetime_max_version != record_message.get('version'):
            LOGGER.debug('WARNING: Stream `{}` dropping record due to version mismatch. Expected: `{}`, Got: `{}`'.format(
                self.stream,
                self.__lifetime_max_version,
                record_message.get('version')
            ))
            return None

        try:
            self.validator.validate(record_message['record'])
        except ValidationError as error:
            add_record = False
            self.invalid_records.append((error, record_message))

        if add_record:
            self.__buffer.append(record_message)
            self.__size += get_size(record_message)
            self.__count += 1
        elif self.invalid_records_detect \
                and len(self.invalid_records) >= self.invalid_records_threshold:
            raise SingerStreamError(
                'Invalid records detected above threshold: {}. See `.args` for details.'.format(
                    self.invalid_records_threshold),
                self.invalid_records)

        self._debug_report_on_buffer_sizes()

    def peek_buffer(self):
        return self.__buffer

    def get_batch(self):
        current_time = arrow.get().format('YYYY-MM-DD HH:mm:ss.SSSSZZ')

        records = []
        for record_message in self.peek_buffer():
            record = record_message['record']

            if 'version' in record_message:
                record[SINGER_TABLE_VERSION] = record_message['version']

            if 'time_extracted' in record_message and record.get(SINGER_RECEIVED_AT) is None:
                record[SINGER_RECEIVED_AT] = record_message['time_extracted']

            if self.use_uuid_pk and record.get(SINGER_PK) is None:
                record[SINGER_PK] = str(uuid.uuid4())

            record[SINGER_BATCHED_AT] = current_time

            if 'sequence' in record_message:
                record[SINGER_SEQUENCE] = record_message['sequence']
            else:
                record[SINGER_SEQUENCE] = arrow.get().timestamp

            records.append(record)

        return records

    def flush_buffer(self):
        self.__buffer = []
        self.__size = 0
        self.__count = 0

    def peek_invalid_records(self):
        return self.invalid_records
