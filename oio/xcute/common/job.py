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

import pickle
import random
from datetime import datetime

from oio.common.easy_value import int_value
from oio.common.green import ratelimit, sleep
from oio.common.json import json
from oio.common.logger import get_logger
from oio.xcute.common.backend import XcuteBackend


def uuid():
    return datetime.utcnow().strftime('%Y%m%d%H%M%S%f') \
        + '-%011X' % random.randrange(16**11)


class XcuteJob(object):
    """
    Dispatch tasks on the platform.
    """

    JOB_TYPE = None
    DEFAULT_ITEM_PER_SECOND = 30
    DEFAULT_DISPATCHER_TIMEOUT = 300

    def __init__(self, conf, orchestrator, job_info,
                 create=False, logger=None):
        self.conf = conf
        self.orchestrator = orchestrator
        self.logger = logger or get_logger(self.conf)
        self.running = True
        self.success = True
        self.sending_tasks = None
        self.sending_job_info = True

        # Prepare backend
        self.backend = XcuteBackend(self.conf)

        # Job info / config
        self.job_info = job_info or dict()
        self.job_id = None
        self.items_sent = 0
        self.items_last_sent = None
        self.items_processed = 0
        self.items_expected = None
        self.errors_total = 0
        self.errors_details = dict()
        self._parse_job_info(create=create)

    def _parse_job_info(self, create=False):
        job_job = self.job_info.setdefault('job', dict())
        if job_job.get('type') != self.JOB_TYPE:
            raise ValueError('Wrong job type')
        if create:
            self.job_id = uuid()
            job_job['id'] = self.job_id
        else:
            self.job_id = job_job.get('id')
            if not self.job_id:
                raise ValueError('Missing job ID')

        job_items = self.job_info.setdefault('items', dict())
        self.items_sent = int_value(job_items.get('sent'), 0)
        job_items['sent'] = self.items_sent
        self.items_last_sent = job_items.get('last_sent')
        self.items_processed = int_value(job_items.get('processed'), 0)
        job_items['processed'] = self.items_processed
        self.items_expected = int_value(job_items.get('expected'), None)
        job_items['expected'] = self.items_expected

        job_errors = self.job_info.setdefault('errors', dict())
        self.errors_total = int_value(job_errors.get('total'), 0)
        job_errors['total'] = self.errors_total
        for key, value in job_errors.items():
            if key == 'total':
                continue
            self.errors_details[key] = int_value(value, 0)
            job_errors[key] = self.errors_details[key]

        job_config = self.job_info.setdefault('config', dict())
        self.max_items_per_second = int_value(
            job_config.get('items_per_second'),
            self.DEFAULT_ITEM_PER_SECOND)
        job_config['items_per_second'] = self.max_items_per_second

    def exit_gracefully(self):
        self.logger.warn('Stop sending and wait for all tasks already sent')
        self.success = False
        self.running = False

    def exit_immediately(self):
        self.logger.warn(
            'Stop sending and not wait for all tasks already sent')
        self.success = False
        self.running = False
        self.wait_results = False

    def _get_tasks_with_args(self):
        raise NotImplementedError()

    def _beanstlkd_job_data_from_task(self, task_class, item, kwargs):
        beanstlkd_job = dict()
        beanstlkd_job['job_id'] = self.job_id
        beanstlkd_job['task'] = pickle.dumps(task_class)
        beanstlkd_job['item'] = item
        beanstlkd_job['kwargs'] = kwargs or dict()
        beanstlkd_job['beanstalkd_reply'] = {
            'addr': self.orchestrator.beanstalkd_reply.addr,
            'tube': self.orchestrator.beanstalkd_reply.tube}
        return json.dumps(beanstlkd_job)

    def _send_task(self, task_with_args, next_worker):
        """
        Send the task through a non-full sender.
        """
        _, item, _ = task_with_args
        beanstlkd_job_data = self._beanstlkd_job_data_from_task(
            *task_with_args)
        workers = self.orchestrator.beanstalkd_workers.values()
        nb_workers = len(workers)
        while True:
            for _ in range(nb_workers):
                success = workers[next_worker].send_job(beanstlkd_job_data)
                next_worker = (next_worker + 1) % nb_workers
                if success:
                    self.items_sent += 1
                    self.items_last_sent = item
                    job_info = {
                        'items': {
                            'sent': self.items_sent,
                            'last_sent': self.items_last_sent,
                        }}
                    self.orchestrator.backend.update_job(
                        self.job_id, job_info)
                    return next_worker
            self.logger.warn("All beanstalkd workers are full")
            sleep(5)

    def distribute_tasks(self):
        self.sending_tasks = True

        next_worker = 0
        items_run_time = 0

        try:
            tasks_with_args = self._get_tasks_with_args()
            items_run_time = ratelimit(
                items_run_time, self.max_items_per_second)
            next_worker = self._send_task(
                next(tasks_with_args), next_worker)
            self.sending_tasks = True
            for task_with_args in tasks_with_args:
                items_run_time = ratelimit(items_run_time,
                                           self.max_items_per_second)
                next_worker = self._send_task(task_with_args, next_worker)

                if not self.running:
                    break
        except Exception as exc:
            if not isinstance(exc, StopIteration) and self.running:
                self.logger.error("Failed to distribute tasks: %s", exc)
                self.exit_gracefully()
        finally:
            if self.running:
                self.sending_tasks = False

    def process_reply(self, reply_info):
        self.items_processed += 1

        exc = pickle.loads(reply_info['exc'])
        if exc:
            self.logger.warn(exc)
            self.errors_total += 1
            exc_name = exc.__class__.__name__
            self.errors_details[exc_name] = self.errors_details.get(
                exc_name, 0) + 1

    def get_update_job_info(self):
        job_info = {
            'items': {
                'processed': self.items_processed
            },
            'errors': {
                'total': self.errors_total
            }
        }
        job_errors = job_info['errors']
        for err, nb in self.errors_details.items():
            job_errors[err] = nb
        return job_info

    def is_finished(self):
        """
        Tell if all workers have finished to process their tasks.
        """
        if self.sending_tasks:
            return False

        return self.items_processed >= self.items_sent
