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
from oio.xcute.common.job import XcuteJob
from oio.xcute.common.task import XcuteTask


class BlobMover(XcuteTask):

    def __init__(self, conf, logger):
        super(BlobMover, self).__init__(conf, logger)
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
            chunk['url'] = 'http://' + service_id + '/' + fake_chunk_id
            chunk['real_url'] = 'http://' + service_addr + '/' + fake_chunk_id
            fake_excluded_chunks.append(chunk)
        return fake_excluded_chunks

    def process(self, chunk_url, rawx_timeout=None, min_chunk_size=None,
                max_chunk_size=None, excluded_rawx=None, **kwargs):
        min_chunk_size = min_chunk_size \
            or RawxDecommissionJob.DEFAULT_MIN_CHUNK_SIZE
        max_chunk_size = max_chunk_size \
            or RawxDecommissionJob.DEFAULT_MAX_CHUNK_SIZE
        excluded_rawx = excluded_rawx \
            or RawxDecommissionJob.DEFAULT_EXCLUDED_RAWX

        fake_excluded_chunks = self._generate_fake_excluded_chunks(
            excluded_rawx)

        meta = self.blob_client.chunk_head(chunk_url, timeout=rawx_timeout,
                                           **kwargs)
        container_id = meta['container_id']
        content_id = meta['content_id']
        chunk_id = meta['chunk_id']

        # Maybe skip the chunk because it doesn't match the size constaint
        chunk_size = int(meta['chunk_size'])
        if chunk_size < min_chunk_size:
            self.logger.debug("SKIP %s too small", chunk_url)
            return
        if max_chunk_size > 0 and chunk_size > max_chunk_size:
            self.logger.debug("SKIP %s too big", chunk_url)
            return

        # Start moving the chunk
        try:
            content = self.content_factory.get(container_id, content_id,
                                               **kwargs)
        except ContentNotFound:
            raise OrphanChunk('Content not found')

        new_chunk = content.move_chunk(
            chunk_id, fake_excluded_chunks=fake_excluded_chunks, **kwargs)

        self.logger.info('Moved chunk %s to %s', chunk_url, new_chunk['url'])


class RawxDecommissionJob(XcuteJob):

    JOB_TYPE = 'rawx-decommission'
    DEFAULT_RDIR_FETCH_LIMIT = 1000
    DEFAULT_RDIR_TIMEOUT = 60.0
    DEFAULT_RAWX_TIMEOUT = 60.0
    DEFAULT_MIN_CHUNK_SIZE = 0
    DEFAULT_MAX_CHUNK_SIZE = 0
    DEFAULT_EXCLUDED_RAWX = list()

    def __init__(self, conf, job_info, create=False, logger=None):
        super(RawxDecommissionJob, self).__init__(
            conf, job_info, create=create, logger=logger)

        self.rdir_client = RdirClient(self.conf, logger=self.logger)

    def _parse_job_info(self, create=False):
        super(RawxDecommissionJob, self)._parse_job_info(create=create)

        job_config = self.job_info.setdefault('config', dict())
        self.service_id = job_config.get('service_id')
        if not self.service_id:
            raise ValueError('Missing service ID')

        self.rdir_fetch_limit = int_value(
            job_config.get('rdir_fetch_limit'),
            self.DEFAULT_RDIR_FETCH_LIMIT)
        job_config['rdir_fetch_limit'] = self.rdir_fetch_limit
        self.rdir_timeout = float_value(
            job_config.get('rdir_timeout'), self.DEFAULT_RDIR_TIMEOUT)
        job_config['rdir_timeout'] = self.rdir_timeout
        self.rawx_timeout = float_value(
            job_config.get('rawx_timeout'), self.DEFAULT_RAWX_TIMEOUT)
        job_config['rawx_timeout'] = self.rawx_timeout
        self.min_chunk_size = int_value(
            job_config.get('min_chunk_size'), self.DEFAULT_MIN_CHUNK_SIZE)
        job_config['min_chunk_size'] = self.min_chunk_size
        self.max_chunk_size = int_value(
            job_config.get('max_chunk_size'), self.DEFAULT_MAX_CHUNK_SIZE)
        job_config['max_chunk_size'] = self.max_chunk_size
        excluded_rawx = job_config.get('excluded_rawx') or ''
        job_config['excluded_rawx'] = excluded_rawx
        self.excluded_rawx = [rawx for rawx in excluded_rawx.split(',')
                              if rawx]

        job_job = self.job_info.setdefault('job', dict())
        job_job['lock'] = 'rawx/%s' % self.service_id

    def _get_tasks_with_args(self):
        chunks_info = self.rdir_client.chunk_fetch(
            self.service_id, limit=self.rdir_fetch_limit,
            timeout=self.rdir_timeout, start_after=self.items_last_sent)

        kwargs = {'rawx_timeout': self.rawx_timeout,
                  'min_chunk_size': self.min_chunk_size,
                  'max_chunk_size': self.max_chunk_size,
                  'excluded_rawx': self.excluded_rawx}
        for _, _, chunk_id, _ in chunks_info:
            yield (BlobMover, '/'.join(('http:/', self.service_id, chunk_id)),
                   kwargs)
