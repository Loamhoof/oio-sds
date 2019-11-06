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

import os
import socket

from oio.common.exceptions import ExplicitBury, OioException, OioTimeout
from oio.common.logger import get_logger
from oio.common.green import sleep, threading
from oio.common.json import json
from oio.conscience.client import ConscienceClient
from oio.event.beanstalk import Beanstalk, BeanstalkdListener, BeanstalkdSender
from oio.xcute.common.backend import XcuteBackend
from oio.xcute.jobs import get_job_class


class XcuteOrchestrator(object):

    def __init__(self, conf, verbose):
        self.conf = conf
        self.logger = get_logger(self.conf, verbose=verbose)
        self.backend = XcuteBackend(self.conf, logger=self.logger)
        self.conscience_client = ConscienceClient(self.conf)
        self.jobs = dict()

        # Orchestrator info
        self.orchestrator_id = \
            self.conf.get('orchestrator_id', socket.gethostname())
        self.logger.info('Using orchestrator id %s' % self.orchestrator_id)

        # Beanstalkd reply
        beanstalkd_reply_addr = self.conf.get('beanstalkd_reply_addr')
        if not beanstalkd_reply_addr:
            raise ValueError('Missing beanstalkd reply address')
        beanstalkd_reply_tube = self.conf['beanstalkd_reply_tube']
        if not beanstalkd_reply_tube:
            raise ValueError('Missing beanstalkd reply tube')
        self.beanstalkd_reply = BeanstalkdListener(
            beanstalkd_reply_addr, beanstalkd_reply_tube, self.logger)
        self.logger.info(
            'Beanstalkd %s using tube %s is used for the replies',
            self.beanstalkd_reply.addr, self.beanstalkd_reply.tube)

        # Prepare beanstalkd workers
        self.beanstalkd_workers_tube = self.conf['beanstalkd_workers_tube']
        if not self.beanstalkd_workers_tube:
            raise ValueError('Missing beanstalkd workers tube')
        self.beanstalkd_workers = list()

        self.running = True

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

    def _get_available_beanstalkd_workers(self):
        """
        Get available beanstalkd workers.
        """

        # Get all available beanstalkd
        all_beanstalkd = self.conscience_client.all_services('beanstalkd')
        all_available_beanstalkd = dict()
        for beanstalkd in all_beanstalkd:
            if beanstalkd['score'] <= 0:
                continue
            all_available_beanstalkd[beanstalkd['addr']] = beanstalkd
        if not all_available_beanstalkd:
            raise OioException('No beanstalkd available')

        # Get beanstalk workers
        beanstalkd_workers = dict()
        for beanstalkd in self._locate_tube(all_available_beanstalkd.values(),
                                            self.beanstalkd_workers_tube):
            beanstalkd_worker = BeanstalkdSender(
                beanstalkd['addr'], self.beanstalkd_workers_tube, self.logger)
            beanstalkd_workers[beanstalkd_worker.addr] = beanstalkd_worker
            self.logger.info(
                'Beanstalkd %s using tube %s is used as a worker',
                beanstalkd_worker.addr, beanstalkd_worker.tube)
        if not beanstalkd_workers:
            raise OioException('No beanstalkd worker available')
        return beanstalkd_workers

    def refresh_beanstalkd_workers(self):
        """
        Get all the beanstalkd and their tubes
        """

        while self.running:
            try:
                self.beanstalkd_workers = \
                    self._get_available_beanstalkd_workers()
            except Exception as exc:
                self.logger.error(
                    'Failed to search for beanstalkd workers: %s', exc)
            sleep(5)

        self.logger.info('Exited beanstalkd thread')

    def _process_reply(self, beanstlkd_job_id, beanstlkd_job_data, **kwargs):
        reply_info = json.loads(beanstlkd_job_data)

        beanstalkd_worker_addr = reply_info.get(
            'beanstalkd_worker', dict()).get('addr')
        if not beanstalkd_worker_addr:
            raise ExplicitBury('No beanstalkd worker')
        beanstalkd_worker = self.beanstalkd_workers.get(beanstalkd_worker_addr)
        if not beanstalkd_worker:
            raise ExplicitBury('Unknown beanstalkd worker')
        beanstalkd_worker.job_done()

        job_id = reply_info.get('job_id')
        if not job_id:
            raise ExplicitBury('No job ID')
        job_details = self.jobs.get(job_id)
        if not job_details:
            raise ExplicitBury('Unknown job ID')
        job, _ = job_details
        job.process_reply(reply_info)

        job_info = job.get_update_job_info()
        self.backend.update_job(job_id, job_info)
        if job.is_finished():
            self.backend.finish_job(job_id)

        yield None

    def retrieve_replies(self):
        """
        Process this orchestrator's job replies
        """

        try:
            while self.running:
                try:
                    replies = self.beanstalkd_reply.fetch_job(
                        self._process_reply, timeout=1)
                    for _ in replies:
                        pass
                except OioTimeout:
                    pass
        except Exception as exc:
            self.logger.error('Failed to fetch task results: %s', exc)
            self.exit()

        self.logger.info('Exited listening thread')

    def start_new_jobs(self):
        """
        One iteration of the main loop
        """
        while self.running:
            job_info = self.backend.start_new_job(self.orchestrator_id)
            if not job_info:
                sleep(5)
                continue

            self.logger.info(job_info)
            self.logger.info('Found new job %s (%s)',
                             job_info['job']['id'], job_info['job']['type'])
            try:
                self.handle_new_job(job_info)
            except Exception as exc:
                self.logger.error(
                    'Failed to instantiate job %s (%s): %s',
                    job_info['job']['id'], job_info['job']['type'], exc)

        self.logger.debug('Finished orchestrating loop')

    def handle_new_job(self, job_info):
        """
        Set a new job's configuration
        and get its tasks before dispatching it
        """
        job_class = get_job_class(job_info)
        job = job_class(self.conf, self, job_info, logger=self.logger)
        self.handle_job(job)

    def handle_running_job(self, job_info):
        """
        Read the job's configuration
        and get its tasks before dispatching it
        """
        job_class = get_job_class(job_info)
        job = job_class(self.conf, self, job_info, logger=self.logger)
        self.handle_job(job)

    def handle_job(self, job):
        """
        Get the beanstalkd available for this job
        and start the dispatching thread
        """
        job_thread = threading.Thread(target=job.distribute_tasks)
        self.jobs[job.job_id] = (job, job_thread)
        job_thread.start()

    def run_forever(self):
        """
        Take jobs from the queue and spawn threads to dispatch them
        """

        # gather beanstalkd info
        refresh_beanstalkd_workers_thread = threading.Thread(
            target=self.refresh_beanstalkd_workers)
        refresh_beanstalkd_workers_thread.start()

        self.logger.info('Wait until beanstalkd workers are found')
        while len(self.beanstalkd_workers) == 0:
            sleep(5)

        # restart running jobs
        self.logger.debug('Look for unfinished jobs')
        orchestrator_jobs = \
            self.backend.list_ochestrator_jobs(self.orchestrator_id)
        for job_info in orchestrator_jobs:
            self.logger.info('Found running job %s (%s)',
                             job_info['job']['id'], job_info['job']['type'])
            self.handle_running_job(job_info)

        # start processing replies
        retrieve_replies_thread = threading.Thread(
            target=self.retrieve_replies)
        retrieve_replies_thread.start()

        # start new jobs
        self.start_new_jobs()

        # exiting
        for job, _ in self.jobs.values():
            job.exit_gracefully()
        refresh_beanstalkd_workers_thread.join()
        retrieve_replies_thread.join()
        for _, job_thread in self.jobs.values():
            job_thread.join()

    def exit_gracefully(self, *args, **kwargs):
        if self.running:
            self.logger.info('Exiting gracefully')
            self.running = False
            return

        self.logger.info('Exiting immediately')
        os._exit(1)
