# Copyright (C) 2019 OpenIO SAS, as part of OpenIO SDS
#
# This library is free software; you can redistribute it and/or
# modify it under the terms of the GNU Lesser General Public
# License as published by the Free Software Foundation; either
# version 3.0 of the License, or (at your option) any later version.
#
# This library is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
# Lesser General Public License for more details.
#
# You should have received a copy of the GNU Lesser General Public
# License along with this library.

from oio.blob.client import BlobClient
from oio.common.easy_value import float_value, int_value
from oio.common.exceptions import ContentNotFound, OrphanChunk
from oio.conscience.client import ConscienceClient
from oio.content.factory import ContentFactory
from oio.rdir.client import RdirClient
from oio.xcute.common.job import XcuteJob, XcuteTask


class BlobMover(XcuteTask):

    def __init__(self, conf, logger=None):
        super(BlobMover, self).__init__(conf, logger=logger)
        self.blob_client = BlobClient(
            self.conf, logger=self.logger)
        self.content_factory = ContentFactory(conf)
        self.conscience_client = ConscienceClient(
            self.conf, logger=self.logger)

    def _generate_fake_excluded_chunks(self, excluded_rawx):
        fake_excluded_chunks = list()
        fake_chunk_id = '0'*64
        for service_id in excluded_rawx:
            service_addr = self.conscience_client.resolve_service_id(
                'rawx', service_id)
            chunk = dict()
            chunk['hash'] = '0000000000000000000000000000000000'
            chunk['pos'] = '0'
            chunk['size'] = 1
            chunk['score'] = 1
            chunk['url'] = 'http://{}/{}'.format(service_id, fake_chunk_id)
            chunk['real_url'] = 'http://{}/{}'.format(service_addr, fake_chunk_id)
            fake_excluded_chunks.append(chunk)
        return fake_excluded_chunks

    def process(self, chunk_id, task_payload):
        rawx_id = task_payload['rawx_id']
        rawx_timeout = task_payload['rawx_timeout']
        min_chunk_size = task_payload['min_chunk_size']
        max_chunk_size = task_payload['max_chunk_size']
        excluded_rawx = task_payload['excluded_rawx']

        fake_excluded_chunks = self._generate_fake_excluded_chunks(
            excluded_rawx)

        chunk_url = 'http://{}/{}'.format(rawx_id, chunk_id)
        meta = self.blob_client.chunk_head(chunk_url, timeout=rawx_timeout)
        container_id = meta['container_id']
        content_id = meta['content_id']
        chunk_id = meta['chunk_id']
        chunk_size = int(meta['chunk_size'])

        # Maybe skip the chunk because it doesn't match the size constaint
        if chunk_size < min_chunk_size:
            self.logger.debug("SKIP %s too small", chunk_url)
            return
        if max_chunk_size > 0 and chunk_size > max_chunk_size:
            self.logger.debug("SKIP %s too big", chunk_url)
            return

        # Start moving the chunk
        try:
            content = self.content_factory.get(container_id, content_id)
        except ContentNotFound:
            raise OrphanChunk('Content not found')

        new_chunk = content.move_chunk(
            chunk_id, fake_excluded_chunks=fake_excluded_chunks)

        self.logger.info('Moved chunk %s to %s', chunk_url, new_chunk['url'])
        return True, chunk_size


class RawxDecommissionJob(XcuteJob):

    JOB_TYPE = 'rawx-decommission'
    DEFAULT_RDIR_FETCH_LIMIT = 1000
    DEFAULT_RDIR_TIMEOUT = 60.0
    DEFAULT_RAWX_TIMEOUT = 60.0
    DEFAULT_MIN_CHUNK_SIZE = 0
    DEFAULT_MAX_CHUNK_SIZE = 0

    @staticmethod
    def sanitize_params(params):
        if not params.get('rawx_id'):
            return ValueError('Missing rawx ID')

        sanitized_params = params.copy()

        sanitized_params['rdir_fetch_limit'] = int_value(
            params.get('rdir_fetch_limit'),
            RawxDecommissionJob.DEFAULT_RDIR_FETCH_LIMIT)

        sanitized_params['rdir_timeout'] = float_value(
            params.get('rdir_timeout'),
            RawxDecommissionJob.DEFAULT_RDIR_TIMEOUT)

        sanitized_params['rawx_timeout'] = float_value(
            params.get('rawx_timeout'),
            RawxDecommissionJob.DEFAULT_RAWX_TIMEOUT)

        sanitized_params['min_chunk_size'] = int_value(
            params.get('min_chunk_size'),
            RawxDecommissionJob.DEFAULT_MIN_CHUNK_SIZE)

        sanitized_params['max_chunk_size'] = int_value(
            params.get('max_chunk_size'),
            RawxDecommissionJob.DEFAULT_MAX_CHUNK_SIZE)

        excluded_rawx = list()
        excluded_rawx_param = params.get('excluded_rawx')
        if excluded_rawx_param:
            excluded_rawx = excluded_rawx_param.split(',')
        sanitized_params['excluded_rawx'] = excluded_rawx

        lock = 'rawx/%s' % params['rawx_id']

        return (sanitized_params, lock)

    @staticmethod
    def get_tasks(conf, logger, params, marker=None):
        rdir_client = RdirClient(conf, logger=logger)

        rawx_id = params['rawx_id']
        rdir_fetch_limit = params['rdir_fetch_limit']
        rdir_timeout = params['rdir_timeout']

        chunk_infos = rdir_client.chunk_fetch(
            rawx_id, timeout=rdir_timeout,
            limit=rdir_fetch_limit, start_after=marker)

        for i, (_, _, chunk_id, _) in enumerate(chunk_infos):
            payload = {
                'rawx_id': params['rawx_id'],
                'rawx_timeout': params['rawx_timeout'],
                'min_chunk_size': params['min_chunk_size'],
                'max_chunk_size': params['max_chunk_size'],
                'excluded_rawx': params['excluded_rawx']
            }

            yield (BlobMover, chunk_id, payload, i)

    @staticmethod
    def reduce_result(total_chunk_size, chunk_size):
        if total_chunk_size is None:
            total_chunk_size = 0

        return total_chunk_size + chunk_size