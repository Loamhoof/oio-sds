from oio.xcute.jobs.blob_mover import RawxDecommissionJob
from oio.xcute.jobs.tester import TesterJob

JOB_TYPES = {
    RawxDecommissionJob.JOB_TYPE: RawxDecommissionJob,
    TesterJob.JOB_TYPE: TesterJob
}


def get_job_class(job_info):
    job_type = job_info.get('job', dict()).get('type')
    if not job_type:
        raise ValueError('Missing job type')

    job_class = JOB_TYPES.get(job_type)
    if not job_class:
        raise ValueError('Job type unknown')
    return job_class
