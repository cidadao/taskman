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

    _next_id = 0
    TypeUser = 0
    TypeConfirm = 1
    TypeRetry = 2

    def __init__(self):
        self.name = self.__class__.__name__
        self.id = Task._next_id
        self.desc = ""
        self.type = Task.TypeUser
        self.settings = {}
        self._cond_proceed = threading.Condition()
        self._callbacks = {
            "message": self._handle_message
        }
        self._loader_id = -1
        Task._next_id += 1

    def _set_settings(self, settings):
        self.settings = settings

    def set_callback(self, name, cb):
        self._callbacks[name] = cb

    def _handle_message(self, message):
        logger.log('d', "Message: %s", message)

    def message(self, text, params={}):
        self._callbacks["message"]({
            "message": {
                "origin": "task",
                "text": text,
                "params": params,
                "task": {"name": self.name, "loader_id": self._loader_id}
            }
        })

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

    def __init__(self, message_text):
        super(ConfirmTask, self).__init__()
        self.type = Task.TypeConfirm
        self.message_text = message_text

    def run(self):
        self.message(self.message_text)
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
        self.message("Retry?")
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
            "message": self._handle_message,
            "handle_abort": None
        }
        self.event_done = threading.Event()
        self._deps = deps

        if loader_settings is not None:
            for key in loader_settings.keys():
                self.settings[key] = loader_settings[key]

        for task in tasks:
            task._loader_id = self.id
            if tasks_settings is not None:
                task._set_settings(tasks_settings)


    def set_callback(self, name, cb, extend_to_tasks=False):
        self._callbacks[name] = cb
        if extend_to_tasks:
            for task in self.tasks:
                if name in task._callbacks.keys():
                    task.set_callback(name, cb)

    def _handle_message(self, message):
        logger.log('d', "Message: %s", message)

    def message(self, text, params={}):
        self._callbacks["message"]({
            "message": {
                "origin": "loader",
                "text": text,
                "params": params,
                "loader": {"id": self.id}
            }
        })

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
            self.message("loader.wait", {"dep_id": dep.id})
            while not dep.event_done.is_set():
                dep.event_done.wait()

        logger.log('d', "TaskLoader %d run" % self.id)
        self.message("loader.running")
        self.run_iteration += 1

        tasks_count = len(self.tasks)
        last_failed = False

        it = 0
        while it < tasks_count:
            task = self.tasks[it]
            self.current_task = task
            logger.log('d', "%s" % task.name)
            self.message("loader.current_task", {"task_name": task.name})
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
        self.message("loader.done")
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
            "loader.list": self.loader_list,
            "loader.run_all": self.loader_runall,
            "loader.run": self.loader_run,
            "task.proceed": self.task_proceed,
            "task.retry": self.task_retry
        }

        for task_loader in self._task_loaders:
            task_loader.set_callback("message", self._handle_message, extend_to_tasks=True)

    def _handle_message(self, text):
        logger.log('d', "Message: %s" % text)
        for skt in self._clients:
            skt.send(str(text) + "\n\r")

    def message(self, text, params={}):
        self._handle_message({
            "message": {
                "origin": "controller",
                "text": text,
                "params": params,
            }
        })

    def quit(self):
        self._request_to_quit = True

    def loader_list(self):
        logger.log('d', "TaskLoader LIST BEGIN")
        for task_loader in self._task_loaders:
            logger.log('d', " ID: %d" % task_loader.id)
            for task in task_loader.tasks:
                logger.log('d', "  %s" % task.name)
        logger.log('d', "TaskLoader LIST END")

    def loader_runall(self):
        for task_loader in self._task_loaders:
            threading._start_new_thread(task_loader.run, ())

    def loader_run(self, id):
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

    def send_data(self, data):
        data_pkt = {
            "data": data
        }
        self._queue.put(data_pkt)

    def run(self, count=0):
        logger.log('d', "TaskController is running...")
        while self._request_to_quit is False:
            pkt = self._queue.get()
            pkt_keys = pkt.keys()
            if "rpc" in pkt_keys:
                self._handle_rpc(pkt)
            elif "data" in pkt_keys:
                for skt in self._clients:
                    skt.send(pkt)

    def connect(self, hostname, port):
        self._alive = True
        self._server.bind((hostname, port))
        self._server.listen(1)
        self._thread_listener.start()
        self.message("server.connected")

    def disconnect(self):
        self._alive = False
        #self._thread_listener.join(10000)
        self.message("server.disconnected")
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
        if "rpc" not in rpc.keys():
            # Invalid RPC. And that's fine.
            logger.log('d', "invalid RPC: \"method\" element not found")
            pass
        rpc = rpc["rpc"]
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
        logger.log('d', "Client connected: %s %d" % (addr[0], skt.fileno()))

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
                rpc_pkt = {
                    "rpc": {
                        "method": method,
                        "params": params,
                    }
                }
                self._queue.put(rpc_pkt)

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
        logger.log('i', "Client disconnected: %s %d" % (addr[0], skt.fileno()))


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
