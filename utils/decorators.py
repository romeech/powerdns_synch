import sys
import time

import logging

logger = logging.getLogger(__name__)


def save_traceback():
    import traceback
    logger.debug(str(sys.exc_info()))
    logger.debug(traceback.format_exc())


def log_remote_errors(func):
    def _safe_func(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except:
            save_traceback()
            logger.error(
                "Failed to perform remote operation '{}', with args: {}, {}, reason: {}",
                func.func_name, args, kwargs, sys.exc_info()[0]
            )
            raise
    _safe_func.func_name = func.func_name
    return _safe_func


def log_start_end(func):
    def _logged_func(*args, **kwargs):
        def get_display_view(arg):
            if isinstance(arg, list):
                return "<list:len={}>".format(len(arg))
            elif isinstance(arg, set):
                return "<set:len={}>".format(len(arg))
            elif isinstance(arg, dict):
                return "<set:len={}>".format(len(arg))
            else:
                return arg

        display_args = [get_display_view(a) for a in args]
        display_kwargs = {k: get_display_view(v) for k, v in kwargs}

        logger.debug("Enter function '{}', args: {}, kwargs: {}"
                     .format(func.func_name, display_args, display_kwargs))
        try:
            return func(*args, **kwargs)
        except:
            raise
        finally:
            logger.debug("Exit from function '{}'".format(func.func_name))
    _logged_func.func_name = func.func_name
    return _logged_func


def measure_worktime(f):
    def _wrap(*args):
        time1 = time.time()
        ret = f(*args)
        time2 = time.time()
        print '{} function took {:0.3f} ms'.format(f.func_name, (time2 - time1) * 1000.0)
        return ret
    _wrap.func_name = f.func_name
    return _wrap
