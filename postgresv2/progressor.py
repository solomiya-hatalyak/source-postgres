from concurrent.futures import ThreadPoolExecutor
from functools import wraps
from threading import Event


def background_progress(message, waiting_interval=10*60):
    """ A decorator is used to emit progress while long operation is executed.
        For example, for database's data sources such operations might be
        declaration of the cursor or counting number of rows.
        This decorator should only be used on methods that are waiting for
        input/output operations to be completed.

       Parameters
       ----------
       message : str
           Message that will be emitted while waiting for operation to complete.
       waiting_interval : float
           Time in seconds to wait between progress emitting.
           Defaults to 10 minutes
    """
    def _background_progress(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            self = args[0]
            self.log('Creating background progress emitter')
            finished = Event()
            with ThreadPoolExecutor(max_workers=1) as executor:
                func_future = executor.submit(func, *args, **kwargs)
                func_future.add_done_callback(lambda future: finished.set())

                while not func_future.done():
                    self.log(message)
                    self.progress(None, None, message)
                    finished.wait(timeout=waiting_interval)

                return func_future.result()

        return wrapper

    return _background_progress
