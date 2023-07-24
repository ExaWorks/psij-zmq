"""This module contains the ZMQService :class:`~psij.JobExecutor`."""

import threading
from pathlib import Path
from typing import Optional, List, Dict, Any

from psij import (
    Job,
    JobExecutorConfig,
    JobState,
    JobStatus,
    JobExecutor,
    Export,
)

import psij_ssh

import radical.utils as ru


class ZMQExecutorConfig(JobExecutorConfig):
    """A configuration class for :class:`~ZMQExecutor` implementations.
    """

    def __init__(self, launcher_log_file: Optional[Path] = None,
                 work_directory: Optional[Path] = None,
                 bootstrap_service: Optional[bool] = False,
                 ve_path: Optional[str] = None,
                 ):
        """Initializes a zmq executor configuration.

        Parameters
        ----------
        launcher_log_file
            See :class:`~psij.JobExecutorConfig`.
        work_directory
            See :class:`~psij.JobExecutorConfig`.
        bootstrap_service
            If set to `True`, bootstrap the ZMQ service on the target resource.
            That might imply the creation of a virtualenv on that resource.
        ve_path
            If specified, the path to create a virtuale environment at in case
            `bootstrap_service` is set to `True`.  If not specified, default to
            `$HOME/.psij/ve`.
        """
        super().__init__(work_directory, launcher_log_file)
        self.bootstrap_service = bootstrap_service
        self.ve_path = ve_path


class ZMQExecutor(JobExecutor):
    """A :class:`~psij.JobExecutor` for a PSI/J ZMQ service endpoint.

    This executor forwards all requests to a ZMQ service endpoint which is then
    executing the respective request on the target resource.
    """

    _state_map = {'NEW': JobState.NEW,
                  'QUEUED': JobState.QUEUED,
                  'ACTIVE': JobState.ACTIVE,
                  'COMPLETED': JobState.COMPLETED,
                  'FAILED': JobState.FAILED,
                  'CANCELED': JobState.CANCELED}

    _final = [JobState.COMPLETED, JobState.FAILED, JobState.CANCELED]

    def __init__(
        self, url: Optional[str] = None, config: Optional[ZMQExecutorConfig] = None
    ) -> None:
        """
        Initializes a `ZMQServiceJobExecutor`.

        :param url: address at which to contact the remote service.
                    Supported schemas: `tcp://` and `zmq://`
        :param config: The `ZMQServiceJobExecutor` does not have any
                    configuration options.
        """
        if not config:
            config = ZMQExecutorConfig()

        self._sub = None
        ru_url = ru.Url(url)

        self._use_ssh = False
        schemas = ru_url.schema.split('+')
        if 'ssh' in schemas:
            self._use_ssh = True

        if 'zmq' not in schemas and 'tcp' not in schemas:
            raise ValueError('upported url schema %s (no zmq)' % ru_url.schema)

        schemas.remove('ssh')

        if 'zmq' in schemas:
            schemas.remove('zmq')
            schemas.append('tcp')

        if not schemas:
            new_schema = 'local'

        else:
            new_schema = '+'.join(schemas)

        super().__init__(url=str(ru_url), config=config)

        self._jobs: Dict[str, Job] = dict()
        self._idmap: Dict[str, str] = dict()
        self._serialize = Export()

        # we can only update the idmap when the `submit` request returns, but
        # an state notification may happen before that.  We use a lock to ensure
        # that state updates are delayed until after `submit` completed.
        self._lock = threading.Lock()

        self._bootstrap = config.bootstrap_service
        self._ve_path = config.ve_path

        self._bootstrap = True  # FIXME

        # If ssh is used: get a connection
        self._conn = None
        if self._use_ssh:
            ssh_url = ru.Url(ru_url)
            ssh_url.schema = 'ssh'

            self._conn = psij_ssh.SSHConnection(str(ssh_url), config)

        else:
            self._conn = psij_ssh.LocalConnection(ru_url, config)

        if not self._bootstrap:
            # the given URL is the service endpoint - use as is
            service_url = ru_url
            service_url.schema = new_schema

        else:
            # If bootstrap was requested:
            #   - esure ve exists or is created
            #   - ensure psij_zmq is installed
            #   - start the service
            modules = ['psij_zmq', '/home/merzky/j/ru.2']
            if config.ve_path:
                self._ve_path = self._conn.bootstrap(modules=modules,
                                                     rem_path=config.ve_path)
            else:
                self._ve_path = self._conn.bootstrap(modules=modules)

            # the ve is prepared - launch the service instance
            cmd = 'radical-utils-service -n test -c psij_zmq_service.py | tee /tmp/out'
            ret, out, err = self._conn.run(cmd)

            print('>>>>>>>>>>>>>>>>>>>>>> ret:', ret, type(ret))

            if ret:
                print('## ret: --%s--' % ret)
                print('## out: --%s--' % out)
                print('## err: --%s--' % err)
                raise RuntimeError('server launch failed: %s\n%s' % (out, err))

            print('>>>>>>>>>>>>>>>>>>>>> out:', out)
            print('>>>>>>>>>>>>>>>>>>>>> err:', err)

        # Once we have the ve and the service runs, connect to it.  If ssh is
        # used, connect to a local tunnel endpoint.

            # FIXME

        # connect to service and register this client instance
        ru_url.schema = new_schema
        print(str(ru_url))
        self._client = ru.zmq.Client(url=str(ru_url).rstrip('/'))
        self._cid, sub_url = self._client.request('register', name=new_schema)

        # FIXME: call back to hosting ssh executor, get tunnel to sub_url

        # subscribe for state update information (trigger `self._state_cb`)
        self._sub = ru.zmq.Subscriber(channel='state', url=sub_url,
                                      cb=self._state_cb, topic=self._cid)

    def __del__(self) -> None:
        """Stop subscriber thread upon destruction."""
        if self._sub is not None:
            self._sub.stop()

    def _state_cb(self, topic: str, msg: Dict[str, Any]) -> None:
        """Callback triggered on job state update messages.

        Update the status of the psij.Job.
        """
        assert topic == self._cid, str([topic, self._cid, msg])

        with self._lock:
            jobid = self._idmap.get(msg['jobid'])
            if not jobid:
                # FIXME: use logger
                print('job %s unknown: %s' % (jobid, self._idmap.keys()))
                return

        job = self._jobs.get(jobid)
        assert job

        state = self._state_map[msg['state']]
        status = JobStatus(state, time=msg['time'], message=msg['message'],
                           exit_code=msg['exit_code'], metadata=msg['metadata'])
        self._set_job_status(job, status)

        if state in self._final:
            del self._jobs[jobid]

    def submit(self, job: Job) -> None:
        """See :func:`~psij.job_executor.JobExecutor.submit`."""
        job.executor = self
        with self._lock:
            self._jobs[job.id] = job
            rep = self._client.request('submit', cid=self._cid,
                                       spec=self._serialize.to_dict(job.spec))
            job._native_id = str(rep)
            self._idmap[job._native_id] = job.id

    def cancel(self, job: Job) -> None:
        """See :func:`~psij.job_executor.JobExecutor.cancel`."""
        self._client.request('cancel', cid=self._cid, jobid=job._native_id)

    def list(self) -> List[str]:
        """See :func:`~psij.job_executor.JobExecutor.list`.

        Return a list of ids representing jobs that are running on the
        underlying implementation.  We consider the remote service's job ids as
        native job ids and return them unaltered.

        :return: The list of known job ids.
        """
        ret = list()
        for val in self._client.request('list', cid=self._cid):
            ret.append(str(val))
        return ret

    def attach(self, job: Job, native_id: str) -> None:
        """
        Attaches a job instance to an existing job.

        The job instance must be in the :attr:`~psij.JobState.NEW` state.

        :param job: The job instance to attach.
        :param native_id: The native ID of the backend job to attached to, as
          obtained through the `:func:list` method.
        """
        assert job.status.state == JobState.NEW
        job.executor = self
        job._native_id = native_id
        self._idmap[native_id] = job.id
        self._jobs[job.id] = job
