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

import random

import oio.common.exceptions as exc
from oio.common.easy_value import int_value
from oio.xcute.common.job import XcuteJob
from oio.xcute.common.task import XcuteTask


EXCEPTIONS = [exc.BadRequest,
              exc.Forbidden,
              exc.NotFound,
              exc.MethodNotAllowed,
              exc.Conflict,
              exc.ClientPreconditionFailed,
              exc.TooLarge,
              exc.UnsatisfiableRange,
              exc.ServiceBusy]


ITEMS = list()
for i in range(1000):
    ITEMS.append('myitem-' + str(i))


class Tester(XcuteTask):

    def process(self, item, error_percentage=None, **kwargs):
        if error_percentage and random.randrange(100) < error_percentage:
            exc_class = random.choice(EXCEPTIONS)
            raise exc_class()
        self.logger.error('It works (item=%s ; kwargs=%s) !!!',
                          item, str(kwargs))


class TesterJob(XcuteJob):

    JOB_TYPE = 'tester'
    DEFAULT_ERROR_PERCENTAGE = 0

    def _parse_job_info(self, resume=False):
        super(TesterJob, self)._parse_job_info(resume=resume)

        job_config = self.job_info.setdefault('config', dict())
        self.job_lock = job_config.get('job_lock')
        self.error_percentage = int_value(
            job_config.get('error_percentage'),
            self.DEFAULT_ERROR_PERCENTAGE)
        job_config['error_percentage'] = self.error_percentage

        job_job = self.job_info.setdefault('job', dict())
        job_job['lock'] = self.job_lock

    def _get_tasks_with_args(self):
        start_index = 0
        if self.items_last_sent is not None:
            start_index = ITEMS.index(self.items_last_sent) + 1

        kwargs = {'lock': self.job_lock,
                  'error_percentage': self.error_percentage}
        for item in ITEMS[start_index:]:
            yield (Tester, item, kwargs)
