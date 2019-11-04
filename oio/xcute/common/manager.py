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

from oio.common.logger import get_logger
from oio.xcute.common.backend import XcuteBackend
from oio.xcute.blob_mover import RawxDecommissionJob
from oio.xcute.tester import TesterJob


class XcuteManager(object):

    JOB_TYPES = {
        RawxDecommissionJob.JOB_TYPE: RawxDecommissionJob,
        TesterJob.JOB_TYPE: TesterJob
    }

    def __init__(self, logger=None):
        self.conf = dict()
        # TODO(adu): Remove this
        self.conf['redis_host'] = '127.0.0.1:6379'
        self.conf['namespace'] = 'OPENIO'
        self.logger = logger or get_logger(self.conf)
        self.backend = XcuteBackend(self.conf)

    def create_job(self, job_info):
        try:
            job_class = self.JOB_TYPES[job_info['job']['type']]
        except KeyError:
            raise ValueError('Job type unknown')

        return job_class(self.conf, job_info, logger=self.logger)

    def resume_job(self, job_id):
        job_info = self.backend.get_job_info(job_id)

        try:
            job_class = self.JOB_TYPES[job_info['job']['type']]
        except KeyError:
            raise ValueError('Job type unknown')

        return job_class(self.conf, job_info, resume=True, logger=self.logger)

    def list_jobs(self):
        return self.backend.list_jobs()

    def show_job(self, job_id):
        return self.backend.get_job_info(job_id)

    def delete_job(self, job_id):
        self.backend.delete_job(job_id)

    def get_job_config(self, job_id):
        return self.backend.get_job_config(job_id)

    def get_locks(self):
        return self.backend.get_locks()
