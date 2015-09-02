__version__ = "0.1.0"

import threading
import socket
import select
import Queue
import json
import inspect

from utils import SerialParser
import logger



class Task(object):

    next_id = 0
    TypeUser = 0
    TypeConfirm = 1
    TypeRetry = 2

    ProceedUnknown = 255
    ProceedOK = 0
    ProceedKO = 1

    def __init__(self):
        self.name = self.__class__.__name__
        self.desc = ""
        self.type = Task.TypeUser
        self.settings = {}
        self._cond_proceed = threading.Condition()
        Task.next_id += 1

    def _set_settings(self, settings):
        self.settings = settings

    def prereq(self):
        return 0

    def check_done(self):
        return False

    def clean(self):
        return

    def reset(self):
        return

    def run(self):
        return 0

class ConfirmTask(Task):

    def __init__(self, message):
        super(ConfirmTask, self).__init__()
        self.type = Task.TypeConfirm
        self.message = message

    def run(self):
        logger.log('d', "Confirm: %s" % self.message)
        self._cond_proceed.acquire()
        self._cond_proceed.wait()
        self._cond_proceed.release()
        return 0

    def proceed(self):
        self._cond_proceed.acquire()
        self._cond_proceed.notify()
        self._cond_proceed.release()

class RetryTask(Task):

    def __init__(self, rewind):
        super(RetryTask, self).__init__()
        self.type = Task.TypeRetry
        self.rewind = rewind
        self._answer = ""

    def run(self):
        self._cond_proceed.acquire()
        self._cond_proceed.wait()
        self._cond_proceed.release()
        logger.log('d', "Answer: %s" % self._answer)
        if self._answer == "y":
            logger.log('d', "Rewind: %d" % self.rewind)
            return self.rewind
        else:
            return 0

    def retry(self, answer):
        self._cond_proceed.acquire()
        self._answer = answer
        self._cond_proceed.notify()
        self._cond_proceed.release()


class TaskLoader():

    _nextID = 0

    def __init__(self, tasks, tasks_settings=None, loader_settings=None, deps=[]):
        self.id = TaskLoader._nextID
        TaskLoader._nextID += 1

        self.tasks = tasks
        self.current_task = None
        self.run_iteration = 0
        self.settings = {
            "verbose":  False,
            "log_en": False,
            "retries_en": True,
            "abort_on_fail": False
        }
        self._callbacks = {
            "handle_abort": None
        }
        self.event_done = threading.Event()
        self._deps = deps

        if loader_settings is not None:
            for key in loader_settings.keys():
                self.settings[key] = loader_settings[key]

        if tasks_settings is not None:
            for task in tasks:
                task._set_settings(tasks_settings)

    def set_callback(self, name, cb):
        self._callbacks[name] = cb

    def reset(self):
        for task in self.tasks:
            task.reset()

    def clean(self):
        for task in self.tasks:
            task.clean()

    def prereq(self):
        tasks_count = len(self.tasks)

        it = 0
        while it < tasks_count:
            task = self.tasks[it]
            task_ret = task.prereq()
            if task_ret != 0:
                return task_ret
            it += 1

        return 0

    def run(self):
        self.event_done.clear()

        for dep in self._deps:
            logger.log('d', "TaskLoader %d waiting for %d" % (self.id, dep.id))
            while not dep.event_done.is_set():
                dep.event_done.wait()

        logger.log('d', "TaskLoader run")
        self.run_iteration += 1

        tasks_count = len(self.tasks)
        last_failed = False

        it = 0
        while it < tasks_count:
            task = self.tasks[it]
            self.current_task = task
            logger.log('d', "%s" % task.name)
            if task.type is Task.TypeRetry:
                if last_failed:
                    rewind = task.run()
                    if rewind > 0:
                        it -= (rewind + 1)
                    last_failed = False

            elif task.check_done() is False:
                task_ret = task.run()
                if task_ret != 0:
                    logger.log('d', "%s FAILED (ret:%d)" % (task.name, task_ret))
                    if self.settings["abort_on_fail"] is True:
                        if self._callbacks["handle_abort"] is not None:
                            self._callbacks["handle_abort"](task, task_ret)
                        return -1
                    if self.settings["retries_en"] is False:
                        return -2
                    else:
                        last_failed = True
            else:
                logger.log('d', "Nothing to be done")
            it += 1

        logger.log('d', "TaskLoader done")
        self.event_done.set()

        return 0


class TaskController:

    def __init__(self, task_loaders):
        self._thread_listener = threading.Thread(target=self._listener)
        self._thread_listener.setDaemon(True)
        self._lock = threading.Lock()
        self._server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._alive = False
        self._clients = []
        self._callback = {}
        self._task_loaders = task_loaders
        self._current_task_loader = None
        self._request_to_quit = False
        self._queue = Queue.Queue()
        self._rpc_callbacks = {
            "quit": self.quit,
            "task_loader.list": self.taskloader_list,
            "task_loader.run_all": self.taskloader_runall,
            "task_loader.run": self.taskloader_run,
            "task.proceed": self.task_proceed,
            "task.retry": self.task_retry
        }

    def quit(self):
        self._request_to_quit = True

    def taskloader_list(self):
        logger.log('d', "TaskLoader LIST BEGIN")
        for task_loader in self._task_loaders:
            logger.log('d', " ID: %d" % task_loader.id)
            for task in task_loader.tasks:
                logger.log('d', "  %s" % task.name)
        logger.log('d', "TaskLoader LIST END")

    def taskloader_runall(self):
        for task_loader in self._task_loaders:
            threading._start_new_thread(task_loader.run, ())

    def taskloader_run(self, id):
        id = int(id)
        logger.log('d', "TaskLoader run id=%d" % id)
        if id >= 0 and (id < len(self._task_loaders)):
            task_loader = self._task_loaders[id]
            self._current_task_loader = task_loader
            threading._start_new_thread(task_loader.run, ())

    def task_proceed(self, id):
        id = int(id)
        for task_loader in self._task_loaders:
            if task_loader.id == id:
                current_task = task_loader.current_task
                if current_task.type == Task.TypeConfirm:
                    current_task.proceed()
                else:
                    logger.log('d', "current task (%s) is not of \"Confirm\" type" % current_task.name)
                return

    def task_retry(self, id, answer):
        id = int(id)
        for task_loader in self._task_loaders:
            if task_loader.id == id:
                current_task = task_loader.current_task
                if current_task.type == Task.TypeRetry:
                    current_task.retry(answer)
                else:
                    logger.log('d', "current task (%s) is not of \"Retry\" type" % current_task.name)
                return

    def _send_data(self, data):
        if len(self._clients) > 0:
            self._clients[0].send(data)

    def send_result(self, id, status):
        self._send_data(json.dumps({
            "result": status,
            "id": id
        }))

    def run(self, count=0):
        logger.log('d', "TaskController is running...")
        while self._request_to_quit is False:
            rpc = self._queue.get()
            self._handle_rpc(rpc)

    def connect(self, hostname, port):
        self._alive = True
        self._server.bind((hostname, port))
        self._server.listen(1)
        self._thread_listener.start()

    def disconnect(self):
        self._alive = False
        #self._thread_listener.join(10000)
        self._server.close()

    def _listener(self):
        while self._alive:
            rd, wr, err = select.select([self._server], [], [])
            for s in rd:
                if s is self._server:
                    client_skt, client_addr = self._server.accept()
                    self._lock.acquire()
                    self._clients.append(client_skt)
                    self._lock.release()
                    threading._start_new_thread(self._handle_client, (client_skt, client_addr))
        self._server.close()
        print("Server closed")

    def _handle_rpc(self, rpc):
        if "method" not in rpc.keys():
            # Invalid RPC. And that's fine.
            logger.log('d', "invalid RPC: \"method\" element not found")
            pass
        rpc_method = rpc["method"]
        rpc_params = rpc["params"]
        if rpc_method in self._rpc_callbacks.keys():
            arg_names = list(inspect.getargspec(self._rpc_callbacks[rpc_method])[0])
            arg_names = arg_names[1:]  # remove the 'self' argument

            # Every argument MUST be passed (no default values)
            if len(rpc_params.keys()) == len(arg_names):
                logger.log('d', "RPC: %s %s" % (rpc_method, rpc_params))
                self._rpc_callbacks[rpc_method](**rpc_params)
            else:
                logger.log('d', "invalid number of arguments: %s (available: %s)" % (rpc_params, arg_names))
        else:
            logger.log('d', "%s is not a Remote Procedure Call" % rpc["method"])

    def _handle_client(self, skt, addr):
        logger.log('d', "handle client %d %s" % (skt.fileno(), addr))

        def _handle_data_in(data):

            data = data.replace('\r', '')
            fields = data.split(' ')
            if len(fields) < 1:
                logger.log('d', "Invalid DATA:", data)
            else:
                method = fields[0]
                params = {}
                for param in fields[1:]:
                    param_fields = param.split("=")
                    if len(param_fields) == 2:
                        param_name = param_fields[0]
                        param_value = param_fields[1]
                        params.update({param_name: param_value})
                rpc = {
                    "method": method,
                    "params": params,
                    "skt": skt
                }
                self._queue.put(rpc)

        data_parser = SerialParser()
        data_parser.set_callback("parsed", _handle_data_in)
        while 1:
            try:
                data = skt.recv(1024)
                if not data:
                    break
                data_parser.parse_data(data)
            except Exception, e:
                logger.log('e', str(e))
                break
        skt.close()
        self._lock.acquire()
        self._clients.remove(skt)
        self._lock.release()
        logger.log('i', "Connection closed: %s" % addr[0])


class TestTask1(Task):

    def run(self):
        print("Do something")
        return 0

class TestTask2(Task):

    def run(self):
        print("End something")
        return 0

class TestError(Task):

    def run(self):
        print("Im an ERROR :)))")
        return -1

if __name__ == "__main__":
    print("Test TaskMan")

    logger.set_levels(logger.ALL)
    logger.set_verbose(True)

    task_loader_0 = TaskLoader([
        TestTask1(),
        ConfirmTask("Are we done?"),
    ])

    task_loader_1 = TaskLoader([
        TestTask1(),
        ConfirmTask("Are you sure?"),
        TestTask2(),
        TestError(),
        RetryTask(2)
    ], deps=[task_loader_0])

    task_controller = TaskController([task_loader_0, task_loader_1])
    task_controller.connect("localhost", 1234)

    task_controller.run()

    task_controller.disconnect()

    print("Bye!")
