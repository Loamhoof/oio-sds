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
from oio.common.exceptions import ExplicitBury, OioException, OioTimeout
from oio.common.green import ratelimit, sleep, threading, time
from oio.common.json import json
from oio.common.logger import get_logger
from oio.conscience.client import ConscienceClient
from oio.event.beanstalk import Beanstalk, BeanstalkdListener, \
    BeanstalkdSender
from oio.xcute.common.backend import XcuteBackend


def uuid():
    return datetime.utcnow().strftime('%Y%m%d%H%M%S%f') \
        + '-%011X' % random.randrange(16**11)


class XcuteJob(object):
    """
    Dispatch tasks on the platform.
    """

    JOB_TYPE = None
    DEFAULT_WORKER_TUBE = 'oio-xcute'
    DEFAULT_ITEM_PER_SECOND = 30
    DEFAULT_DISPATCHER_TIMEOUT = 300

    def __init__(self, conf, job_info, resume=False, logger=None):
        self.conf = conf
        self.logger = logger or get_logger(self.conf)
        self.running = True
        self.success = True
        self.sending_tasks = None
        self.sending_job_info = True
        self.wait_results = True

        # Prepare backend
        self.backend = XcuteBackend(self.conf)

        # Job info / config
        self.job_info = job_info or dict()
        self.job_id = None
        self.items_last_sent = None
        self.items_processed = 0
        self.items_expected = None
        self.errors_total = 0
        self.errors_details = dict()
        self._parse_job_info(resume=resume)

        # Prepare beanstalkd
        conscience_client = ConscienceClient(self.conf)
        all_available_beanstalkd = self._get_all_available_beanstalkd(
            conscience_client)
        # Prepare beanstalkd workers
        try:
            self.beanstalkd_workers = self._get_beanstalkd_workers(
                conscience_client, all_available_beanstalkd)
        except Exception as exc:
            self.logger.error(
                'Failed to search for beanstalkd workers: %s', exc)
            raise
        # Beanstalkd reply
        beanstalkd_reply_addr = self.job_info['beanstalkd_reply']['addr']
        if not beanstalkd_reply_addr:
            try:
                beanstalkd_reply_addr = self._get_beanstalkd_reply_addr(
                    conscience_client, all_available_beanstalkd)
            except Exception as exc:
                self.logger.error(
                    'Failed to search for beanstalkd reply: %s', exc)
                raise
        self.job_info['beanstalkd_reply']['addr'] = beanstalkd_reply_addr
        beanstalkd_reply_tube = self.job_info['beanstalkd_reply']['tube']
        if not resume:
            # If the tube exists, another job must have
            # already used this tube
            tubes = Beanstalk.from_url(
                'beanstalk://' + beanstalkd_reply_addr).tubes()
            if beanstalkd_reply_tube in tubes:
                raise OioException(
                    'Beanstalkd %s using tube %s doesn\'t exist'
                    % (beanstalkd_reply_addr, beanstalkd_reply_tube))
        self.beanstalkd_reply = BeanstalkdListener(
            beanstalkd_reply_addr, beanstalkd_reply_tube, self.logger)
        self.logger.info(
            'Beanstalkd %s using tube %s is selected for the replies',
            self.beanstalkd_reply.addr, self.beanstalkd_reply.tube)

        # Register the job
        if resume:
            self.backend.resume_job(self.job_id, self.job_info)
        else:
            self.backend.start_job(self.job_id, self.job_info)

    def _parse_job_info(self, resume=False):
        job_time = self.job_info.setdefault('time', dict())
        job_time['mtime'] = time.time()

        job_job = self.job_info.setdefault('job', dict())
        if job_job.get('type') != self.JOB_TYPE:
            raise ValueError('Wrong job type')
        if resume:
            self.job_id = job_job.get('id')
            if not self.job_id:
                raise ValueError('Missing job ID')
        else:
            self.job_id = uuid()
            job_job['id'] = self.job_id

        job_beanstalkd_workers = self.job_info.setdefault(
            'beanstalkd_workers', dict())
        beanstalkd_worker_tube = job_beanstalkd_workers.get('tube') \
            or self.DEFAULT_WORKER_TUBE
        job_beanstalkd_workers['tube'] = beanstalkd_worker_tube
        job_beanstalkd_reply = self.job_info.setdefault(
            'beanstalkd_reply', dict())
        beanstalkd_reply_tube = job_beanstalkd_reply.get('tube') \
            or beanstalkd_worker_tube + '.job.reply.' + self.job_id
        job_beanstalkd_reply['tube'] = beanstalkd_reply_tube
        beanstalkd_reply_addr = job_beanstalkd_reply.get('addr')
        job_beanstalkd_reply['addr'] = beanstalkd_reply_addr

        job_items = self.job_info.setdefault('items', dict())
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

    def _get_all_available_beanstalkd(self, conscience_client):
        """
        Get all available beanstalkd.
        """
        all_beanstalkd = conscience_client.all_services('beanstalkd')
        all_available_beanstalkd = dict()
        for beanstalkd in all_beanstalkd:
            if beanstalkd['score'] <= 0:
                continue
            all_available_beanstalkd[beanstalkd['addr']] = beanstalkd
        if not all_available_beanstalkd:
            raise OioException('No beanstalkd available')
        return all_available_beanstalkd

    def _locate_tube(self, services, tube):
        """
        Get a list of beanstalkd services hosting the specified tube.

        :param services: known beanstalkd services.
        :type services: iterable of dictionaries
        :param tube: the tube to locate.
        :returns: a list of beanstalkd services hosting the the specified tube.
        :rtype: `list` of `dict`
        """
        available = list()
        for bsd in services:
            tubes = Beanstalk.from_url(
                'beanstalk://' + bsd['addr']).tubes()
            if tube in tubes:
                available.append(bsd)
        return available

    def _get_beanstalkd_workers(self, conscience_client,
                                all_available_beanstalkd):
        beanstalkd_workers_tube = self.job_info['beanstalkd_workers']['tube']
        beanstalkd_workers = dict()
        for beanstalkd in self._locate_tube(all_available_beanstalkd.values(),
                                            beanstalkd_workers_tube):
            beanstalkd_worker = BeanstalkdSender(
                beanstalkd['addr'], beanstalkd_workers_tube, self.logger)
            beanstalkd_workers[beanstalkd['addr']] = beanstalkd_worker
            self.logger.info(
                'Beanstalkd %s using tube %s is selected as a worker',
                beanstalkd_worker.addr, beanstalkd_worker.tube)
        if not beanstalkd_workers:
            raise OioException('No beanstalkd worker available')
        nb_workers = len(beanstalkd_workers)
        if self.max_items_per_second > 0:
            # Max 2 seconds in advance
            queue_size_per_worker = self.max_items_per_second * 2 / nb_workers
        else:
            queue_size_per_worker = 64
        for _, beanstalkd_worker in beanstalkd_workers.iteritems():
            beanstalkd_worker.low_limit = queue_size_per_worker / 2
            beanstalkd_worker.high_limit = queue_size_per_worker
        return beanstalkd_workers

    def _get_beanstalkd_reply_addr(self, conscience_client,
                                   all_available_beanstalkd):
        local_services = conscience_client.local_services()
        for local_service in local_services:
            if local_service['type'] != 'beanstalkd':
                continue
            local_beanstalkd = all_available_beanstalkd.get(
                local_service['addr'])
            if local_beanstalkd is None:
                continue
            return local_beanstalkd['addr']
        self.logger.warn('No beanstalkd available locally')
        beanstalkd = conscience_client.next_instance('beanstalkd')
        return beanstalkd['addr']

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
            'addr': self.beanstalkd_reply.addr,
            'tube': self.beanstalkd_reply.tube}
        return json.dumps(beanstlkd_job)

    def _send_task(self, task_with_args, next_worker):
        """
        Send the task through a non-full sender.
        """
        _, item, _ = task_with_args
        beanstlkd_job_data = self._beanstlkd_job_data_from_task(
            *task_with_args)
        workers = self.beanstalkd_workers.values()
        nb_workers = len(workers)
        while True:
            for _ in range(nb_workers):
                success = workers[next_worker].send_job(beanstlkd_job_data)
                next_worker = (next_worker + 1) % nb_workers
                if success:
                    self.items_last_sent = item
                    return next_worker
            self.logger.warn("All beanstalkd workers are full")
            sleep(5)

    def _distribute_tasks(self):
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
            self.sending_tasks = False

    def _prepare_job_info(self):
        job_info = {
            'time': {
                'mtime': time.time()
            },
            'items': {
                'last_sent': self.items_last_sent,
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

    def _send_job_info_periodically(self):
        try:
            while self.sending_job_info:
                sleep(1)
                job_info = self._prepare_job_info()
                self.backend.update_job(self.job_id, job_info)
        except Exception as exc:
            self.logger.error("Failed to send job information: %s", exc)
            self.exit_immediately()

    def _all_tasks_are_processed(self):
        """
        Tell if all workers have finished to process their tasks.
        """
        if self.sending_tasks:
            return False

        total_tasks = 0
        for _, worker in self.beanstalkd_workers.iteritems():
            total_tasks += worker.nb_jobs
        return total_tasks <= 0

    def _decode_reply(self, beanstlkd_job_id, beanstlkd_job_data, **kwargs):
        reply_info = json.loads(beanstlkd_job_data)
        if reply_info['job_id'] != self.job_id:
            raise ExplicitBury('Wrong job ID (%d ; expected=%d)'
                               % (reply_info['job_id'], self.job_id))
        yield reply_info

    def _update_job_info(self, reply_info):
        self.items_processed += 1

        exc = pickle.loads(reply_info['exc'])
        if exc:
            self.logger.warn(exc)
            self.errors_total += 1
            exc_name = exc.__class__.__name__
            self.errors_details[exc_name] = self.errors_details.get(
                exc_name, 0) + 1

    def _process_reply(self, reply_info):
        self._update_job_info(reply_info)

        beanstalkd_worker_addr = reply_info['beanstalkd_worker']['addr']
        self.beanstalkd_workers[beanstalkd_worker_addr].job_done()

    def run(self):
        thread_distribute_tasks = threading.Thread(
            target=self._distribute_tasks)
        thread_distribute_tasks.start()

        # Wait until the thread is started sending events
        while self.sending_tasks is None:
            sleep(0.1)

        thread_send_job_info_periodically = threading.Thread(
            target=self._send_job_info_periodically)
        thread_send_job_info_periodically.start()

        # Retrieve replies until all events are processed
        try:
            while self.wait_results and not self._all_tasks_are_processed():
                for _ in range(self.DEFAULT_DISPATCHER_TIMEOUT):
                    if not self.wait_results:
                        break

                    try:
                        replies = self.beanstalkd_reply.fetch_job(
                            self._decode_reply, timeout=1)
                        for reply in replies:
                            self._process_reply(reply)
                        break
                    except OioTimeout:
                        pass
                else:
                    raise OioTimeout('No reply for %d seconds' %
                                     self.DEFAULT_DISPATCHER_TIMEOUT)
        except Exception as exc:
            self.logger.error('Failed to fetch task results: %s', exc)
            self.exit_immediately()

        # Send the last information
        self.sending_job_info = False
        info = self._prepare_job_info()
        self.success = self.errors_total > 0
        thread_send_job_info_periodically.join()
        try:
            if self.running:
                self.backend.finish_job(self.job_id, info)
            else:
                self.backend.pause_job(self.job_id, info)
        except Exception as exc:
            self.logger.error('Failed to send the last information: %s', exc)
            self.success = False
