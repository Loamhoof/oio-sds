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

import redis
from functools import wraps

from oio.common.exceptions import BadRequest, Forbidden, NotFound
from oio.common.green import time
from oio.common.logger import get_logger
from oio.common.redis_conn import RedisConnection
from oio.common.timestamp import Timestamp


def handle_missing_job_id(func):
    @wraps(func)
    def handle_missing_job_id(self, job_id, *args):
        if not job_id:
            raise BadRequest(message='Missing job ID')
        return func(self, job_id, *args)
    return handle_missing_job_id


def handle_missing_update_job_info(func):
    @wraps(func)
    def handle_missing_update_job_info(self, job_id, job_info):
        _job_id = job_info.get('job', dict()).get('id')
        if _job_id is not None and job_id != _job_id:
            raise BadRequest(message='Mismatch job ID')
        return func(self, job_id, job_info)
    return handle_missing_update_job_info


def _flat_dict_from_dict(dict_):
    """
    Create a dictionary without depth.

    {
        'depth0': {
            'depth1': {
                'depth2': 'test'
            }
        }
    }
    =>
    {
        'depth0.depth1.depth2': 'test'
    }
    """
    flat_dict = dict()
    for key, value in dict_.items():
        if not isinstance(value, dict):
            flat_dict[key] = value
            continue

        _flat_dict = _flat_dict_from_dict(value)
        for _key, _value in _flat_dict.items():
            flat_dict[key + '.' + _key] = _value
    return flat_dict


def _dict_from_flat_dict(dict_flat):
    """
    Create a dictionary with depth.

    {
        'depth0.depth1.depth2': 'test'
    }
    =>
    {
        'depth0': {
            'depth1': {
                'depth2': 'test'
            }
        }
    }
    """
    dict_ = dict()
    for key, value in dict_flat.items():
        _key = key
        _dict_ = dict_
        while True:
            _split = _key.split('.', 1)
            if len(_split) == 1:
                break
            _dict_ = _dict_.setdefault(_split[0], dict())
            _key = _split[1]
        _dict_[_key] = value
    return dict_


class XcuteBackend(RedisConnection):

    _lua_errors = {
        'job_exists': (Forbidden,
                       'The job already exists'),
        'no_job': (NotFound,
                   'The job does\'nt exist'),
        'lock_in_use': (Forbidden,
                        'The lock is in use'),
        'must_be_not_waiting': (Forbidden,
                                'The job must be not waiting'),
        'must_be_running': (Forbidden,
                            'The job must be running'),
        'must_be_paused': (Forbidden,
                           'The job must be paused'),
        'must_be_paused_finished': (Forbidden,
                                    'The job must be paused or finished')
        }

    _lua_update_job_info = """
        redis.call('HMSET', 'xcute:job:info:' .. KEYS[1], unpack(ARGV));
        """

    lua_create_job = """
        local job_exists = redis.call('EXISTS', 'xcute:job:info:' .. KEYS[1]);
        if job_exists == 1 then
            return redis.error_reply('job_exists');
        end;

        for i, v in ipairs(ARGV) do
            if math.mod(i,2) == 1 and v == 'job.lock' then
                local lock = ARGV[i+1];
                local lock_in_use = redis.call('HSETNX', 'xcute:job:locks',
                                               lock, KEYS[1]);
                if lock_in_use ~= 1 then
                    return redis.error_reply('lock_in_use');
                end;
                break;
            end;
        end;

        redis.call('HSET', 'xcute:job:info:' .. KEYS[1],
                   'job.status', 'WAITING');
        redis.call('ZADD', 'xcute:job:ids', 0, KEYS[1]);
        redis.call('RPUSH', 'xcute:waiting:queue', KEYS[1])
        """ + _lua_update_job_info

    lua_start_new_job = """
        local job_id = redis.call('LPOP', 'xcute:waiting:queue')
        if job_id == nil or job_id == false then
            return nil;
        end;

        redis.call('HSET', 'xcute:job:info:' .. job_id,
                   'orchestrator.id', KEYS[1]);
        redis.call('ZADD', 'xcute:orchestrator:jobs:' .. KEYS[1], 0, job_id);
        redis.call('HSET', 'xcute:job:info:' .. job_id,
                   'job.status', 'RUNNING');
        return redis.call('HGETALL', 'xcute:job:info:' .. job_id)
        """

    lua_update_job = """
        local status = redis.call('HGET', 'xcute:job:info:' .. KEYS[1],
                                  'job.status');
        if status == nil or status == false then
            return redis.error_reply('no_job');
        end;
        if status == 'WAITING' then
            return redis.error_reply('must_be_not_waiting');
        end;
        """ + _lua_update_job_info

    lua_pause_job = """
        local status = redis.call('HGET', 'xcute:job:info:' .. KEYS[1],
                                  'job.status');
        if status == nil or status == false then
            return redis.error_reply('no_job');
        end;
        if status ~= 'RUNNING' then
            return redis.error_reply('must_be_running');
        end;

        redis.call('HSET', 'xcute:job:info:' .. KEYS[1], 'job.status',
                   'PAUSED');
        """ + _lua_update_job_info

    lua_resume_job = """
        local status = redis.call('HGET', 'xcute:job:info:' .. KEYS[1],
                                  'job.status');
        if status == nil or status == false then
            return redis.error_reply('no_job');
        end;
        if status ~= 'PAUSED' then
            return redis.error_reply('must_be_paused');
        end;

        redis.call('HSET', 'xcute:job:info:' .. KEYS[1], 'job.status',
                   'RUNNING');
        """ + _lua_update_job_info

    lua_finish_job = """
        local status = redis.call('HGET', 'xcute:job:info:' .. KEYS[1],
                                  'job.status');
        if status == nil or status == false then
            return redis.error_reply('no_job');
        end;
        if status ~= 'RUNNING' then
            return redis.error_reply('must_be_running');
        end;

        local lock = redis.call('HGET', 'xcute:job:info:' .. KEYS[1],
                                'job.lock');
        if lock ~= nil and lock ~= false then
            redis.call('HDEL', 'xcute:job:locks', lock);
        end;

        redis.call('HSET', 'xcute:job:info:' .. KEYS[1], 'job.status',
                   'FINISHED');
        """ + _lua_update_job_info

    lua_delete_job = """
        local status = redis.call('HGET', 'xcute:job:info:' .. KEYS[1],
                                  'job.status');
        if status == nil or status == false then
            return redis.error_reply('no_job');
        end;
        if status ~= 'PAUSED' and status ~= 'FINISHED' then
            return redis.error_reply('must_be_paused_finished');
        end;

        local lock = redis.call('HGET', 'xcute:job:info:' .. KEYS[1],
                                'job.lock');
        if lock ~= nil and lock ~= false then
            redis.call('HDEL', 'xcute:job:locks', lock);
        end;

        local orchestrator_id = redis.call(
            'HGET', 'xcute:job:info:' .. KEYS[1], 'orchestrator.id');
        if orchestrator_id ~= nil and orchestrator_id ~= false then
            redis.call('ZREM', 'xcute:orchestrator:jobs:' .. orchestrator_id,
                       KEYS[1]);
        end;

        redis.call('ZREM', 'xcute:job:ids', KEYS[1]);
        redis.call('DEL', 'xcute:job:info:' .. KEYS[1]);
        """

    def __init__(self, conf, logger=None):
        self.conf = conf
        self.logger = logger or get_logger(self.conf)
        redis_conf = {k[6:]: v for k, v in self.conf.items()
                      if k.startswith("redis_")}
        super(XcuteBackend, self).__init__(**redis_conf)

        self.script_create_job = self.register_script(
            self.lua_create_job)
        self.script_start_new_job = self.register_script(
            self.lua_start_new_job)
        self.script_update_job = self.register_script(
            self.lua_update_job)
        self.script_pause_job = self.register_script(
            self.lua_pause_job)
        self.script_resume_job = self.register_script(
            self.lua_resume_job)
        self.script_finish_job = self.register_script(
            self.lua_finish_job)
        self.script_delete_job = self.register_script(
            self.lua_delete_job)

    def list_jobs(self, marker=None, limit=1000):
        jobs = list()
        while True:
            limit_ = limit - len(jobs)
            if limit_ <= 0:
                break
            min = '-'
            if marker:
                max = '(' + marker
            else:
                max = '+'

            job_ids = self.conn.zrevrangebylex(
                'xcute:job:ids', max, min, 0, limit_)

            pipeline = self.conn.pipeline(True)
            for job_id in job_ids:
                pipeline.hgetall('xcute:job:info:%s' % job_id)
            res = pipeline.execute()
            i = 0
            for job_id in job_ids:
                if not res[i]:
                    continue
                jobs.append(_dict_from_flat_dict(res[i]))
                i += 1

            if len(job_ids) < limit_:
                break
        return jobs

    def list_ochestrator_jobs(self, orchestrator_id, marker=None, limit=1000):
        jobs = list()
        while True:
            limit_ = limit - len(jobs)
            if limit_ <= 0:
                break
            min = '-'
            if marker:
                max = '(' + marker
            else:
                max = '+'

            job_ids = self.conn.zrangebylex(
                'xcute:orchestrator:jobs:%s' % orchestrator_id,
                min, max, 0, limit_)

            pipeline = self.conn.pipeline(True)
            for job_id in job_ids:
                pipeline.hgetall('xcute:job:info:%s' % job_id)
            res = pipeline.execute()
            i = 0
            for job_id in job_ids:
                if not res[i]:
                    continue
                jobs.append(_dict_from_flat_dict(res[i]))
                i += 1

            if len(job_ids) < limit_:
                break
        return jobs

    @handle_missing_job_id
    def get_job_info(self, job_id):
        job_info = self.conn.hgetall('xcute:job:info:%s' % job_id)
        if not job_info:
            raise NotFound(message='Job %s doest\'nt exist' % job_id)
        return _dict_from_flat_dict(job_info)

    def get_locks(self):
        return self.conn.hgetall('xcute:job:locks')

    def _run_script(self, job_id, job_info, script):
        script_args = list()
        if job_info:
            for key, value in _flat_dict_from_dict(job_info).items():
                if value is None:
                    continue
                script_args.append(key)
                script_args.append(value)

        try:
            return script(keys=[job_id], args=script_args, client=self.conn)
        except redis.exceptions.ResponseError as exc:
            error = self._lua_errors.get(str(exc))
            if error is None:
                raise
            error_cls, error_msg = error
            raise error_cls(message=error_msg)

    @handle_missing_job_id
    @handle_missing_update_job_info
    def create_job(self, job_id, job_info):
        try:
            if job_info['job']['type'] is None:
                raise KeyError()
        except KeyError:
            raise BadRequest(message='Missing job type')
        ctime = Timestamp(time.time()).normal
        job_time = job_info.setdefault('time', dict())
        job_time['ctime'] = ctime
        job_time['mtime'] = ctime
        self._run_script(job_id, job_info, self.script_create_job)

    def start_new_job(self, orchestrator_id):
        job_info_list = self._run_script(
            orchestrator_id, None, self.script_start_new_job)
        if not job_info_list:
            return None
        job_info = dict()
        key = None
        for value in job_info_list:
            if key is None:
                key = value
            else:
                job_info[key] = value
                key = None
        return _dict_from_flat_dict(job_info)

    @handle_missing_job_id
    @handle_missing_update_job_info
    def update_job(self, job_id, job_info):
        job_info.setdefault('time', dict())['mtime'] = \
            Timestamp(time.time()).normal
        self._run_script(job_id, job_info, self.script_update_job)

    @handle_missing_job_id
    def pause_job(self, job_id):
        job_info = {'time': {'mtime': Timestamp(time.time()).normal}}
        self._run_script(job_id, job_info, self.script_pause_job)

    @handle_missing_job_id
    def resume_job(self, job_id):
        job_info = {'time': {'mtime': Timestamp(time.time()).normal}}
        self._run_script(job_id, job_info, self.script_resume_job)

    @handle_missing_job_id
    def finish_job(self, job_id):
        job_info = {'time': {'mtime': Timestamp(time.time()).normal}}
        self._run_script(job_id, job_info, self.script_finish_job)

    @handle_missing_job_id
    def delete_job(self, job_id):
        job_info = {'time': {'mtime': Timestamp(time.time()).normal}}
        self._run_script(job_id, job_info, self.script_delete_job)
