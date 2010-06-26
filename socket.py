'''
PyDanga Socket
'''

import socket, sys, time, select, errno

########################################################################
class DangaReactor(object):
    
    # Constants from the epoll module
    _EPOLLIN = 0x001
    _EPOLLPRI = 0x002
    _EPOLLOUT = 0x004
    _EPOLLERR = 0x008
    _EPOLLHUP = 0x010
    _EPOLLRDHUP = 0x2000
    _EPOLLONESHOT = (1 << 30)
    _EPOLLET = (1 << 31)

    # Our events map exactly to the epoll events
    EVENT_NONE = 0
    EVENT_READ = _EPOLLIN
    EVENT_WRITE = _EPOLLOUT
    EVENT_ERROR = _EPOLLERR | _EPOLLHUP | _EPOLLRDHUP
    
    """"""

    #----------------------------------------------------------------------
    def __init__(self, pollster=None):
        """Constructor"""
        self._poller = pollster or _poll_obj()
        
        self._descriptor_map = dict()
        self.push_back_set = dict()
        
        self.to_close = []
        self._other_fds = dict()
        self.post_loop_callback = None
        self.plc_map = dict()
        self.loop_timeout = -1
        self.do_profile = False
        self.profiling = dict()
        self.done_init = False
        self._timers = []

    @classmethod
    def instance(cls):
        """Returns a global IOLoop instance.

        Most single-threaded applications have a single, global DangaReactor.
        Use this method instead of passing around IOLoop instances
        throughout your code.

        A common pattern for classes that depend on IOLoops is to use
        a default argument to enable programs with multiple IOLoops
        but not require the argument for simpler applications:

            class MyClass(object):
                def __init__(self, io_loop=None):
                    self.io_loop = io_loop or DangaReactor.instance()
        """
        if not hasattr(cls, "_instance"):
            cls._instance = cls()
        return cls._instance

    @classmethod
    def initialized(cls):
        return hasattr(cls, "_instance")
    
    #----------------------------------------------------------------------
    def reset(self):
        """Reset all state"""
        self._descriptor_map = dict()
        self.push_back_set = dict()
        self.to_close = []
        self._other_fds = dict()
        self.loop_timeout = -1
        self.do_profile = False
        self.profiling = dict()
        self._timers = []
        self.post_loop_callback = None
        self.plc_map = dict()
        self.done_init = False
        
        if self._poller is not None:
            self._poller.close()
            
        #self._poller = self.first_time__poller
        
    #----------------------------------------------------------------------
    def watched_sockets(self):
        """"""
        return self._descriptor_map.keys()
    
    #----------------------------------------------------------------------
    def to_close(self):
        """"""
        return self.to_close
    
    #----------------------------------------------------------------------
    def other_fds(self, fdmap=None):
        """"""
        if (fdmap is not None):
            self._other_fds = fdmap
        return self._other_fds
    
    #----------------------------------------------------------------------
    def add_other_fds(self, fdmap=None):
        """"""
        if fdmap is not None:
            self._other_fds.update(fdmap)
        return self._other_fds
    
    #----------------------------------------------------------------------
    def set_loop_timeout(self, timeout):
        """"""
        self.loop_timeout = timeout
        return self.loop_timeout
    
    #----------------------------------------------------------------------
    def add_timer(self, seconds, callback):
        """"""
        deadline = time.time() + seconds
        timer = _Timer(deadline, callback)
        bisect.insort(self._timers, timer)
        return timer
    
    #----------------------------------------------------------------------
    def get_sock_ref(self):
        """"""
        return self._descriptor_map
    
    #----------------------------------------------------------------------
    def run_timers(self):
        """"""
        if len(self._timers) == 0:
            return self.loop_timeout
        now = time.time()
        while self._timers and self._timers[0].deadline <= now:
            timeout = self._timers.pop(0)
            self._run_callback(timeout.callback)
        
        poll_timeout = self.loop_timeout
        if self._timers:
            milliseconds = self._timers[0].deadline - now
            poll_timeout = min(milliseconds, self.loop_timeout)
        
        return poll_timeout
    
    def _run_callback(self, callback):
        try:
            callback()
        except (KeyboardInterrupt, SystemExit):
            raise
        except:
            self.handle_callback_exception(callback)

    def handle_callback_exception(self, callback):
        """This method is called whenever a callback run by the IOLoop
        throws an exception.

        By default simply logs the exception as an error.  Subclasses
        may override this method to customize reporting of exceptions.

        The exception itself is not passed explicitly, but is available
        in sys.exc_info.
        """
        logging.error("Exception in callback %r", callback, exc_info=True)    
    
    def event_loop(self):        
        if self._stopped:
            self._stopped = False
            return
        
        self._running = True
    
    def add_handler(self, fd, handler, events):
        """Registers the given handler to receive the given events for fd."""
        self._descriptor_map[fd] = handler
        self._poller.register(fd, events | self.ERROR)

    def update_handler(self, fd, events):
        """Changes the events we listen for fd."""
        self._poller.modify(fd, events | self.ERROR)

    def remove_handler(self, fd):
        """Stop listening for events on fd."""
        self._descriptor_map.pop(fd, None)
        self._events.pop(fd, None)
        try:
            self._poller.unregister(fd)
        except (OSError, IOError):
            logging.debug("Error deleting fd from IOLoop", exc_info=True)
        
    
    
########################################################################
class DangaSocket(object):
    """"""

    #----------------------------------------------------------------------
    def __init__(self):
        """Constructor"""
        self.sock = None                    # underlying socket object
        self.fd = None                      # numeric file descriptor
        self.write_buf = None               # 
        self.write_buf_offt = 0
        self.write_buf_size = 0
        self.write_set_watch = False
        self.read_push_back = []
        self.closed = True
        self.corked = False
        self.event_watch = 0
        self.peer_v6 = False
        self.peer_ip = ''
        self.peer_port = 0
        self.local_ip = ''
        self.local_port = 0        
    
class _Timer(object):
    """An IOLoop timeout, a UNIX timestamp and a callback"""

    # Reduce memory overhead when there are lots of pending callbacks
    __slots__ = ['deadline', 'callback']

    def __init__(self, deadline, callback):
        self.deadline = deadline
        self.callback = callback

    def __cmp__(self, other):
        return cmp((self.deadline, id(self.callback)),
                   (other.deadline, id(other.callback)))
    
class _KQueue(object):
    """A kqueue-based event loop for BSD/Mac systems."""
    def __init__(self):
        self._kqueue = select.kqueue()
        self._active = {}

    def fileno(self):
        return self._kqueue.fileno()

    def register(self, fd, events):
        self._control(fd, events, select.KQ_EV_ADD)
        self._active[fd] = events

    def modify(self, fd, events):
        self.unregister(fd)
        self.register(fd, events)

    def unregister(self, fd):
        events = self._active.pop(fd)
        self._control(fd, events, select.KQ_EV_DELETE)

    def _control(self, fd, events, flags):
        kevents = []
        if events & DangaReactor.EVENT_WRITE:
            kevents.append(select.kevent(
                    fd, filter=select.KQ_FILTER_WRITE, flags=flags))
        if events & DangaReactor.EVENT_READ or not kevents:
            # Always read when there is not a write
            kevents.append(select.kevent(
                    fd, filter=select.KQ_FILTER_READ, flags=flags))
        # Even though control() takes a list, it seems to return EINVAL
        # on Mac OS X (10.6) when there is more than one event in the list.
        for kevent in kevents:
            self._kqueue.control([kevent], 0)

    def poll(self, timeout):
        kevents = self._kqueue.control(None, 1000, timeout)
        events = {}
        for kevent in kevents:
            fd = kevent.ident
            flags = 0
            if kevent.filter == select.KQ_FILTER_READ:
                events[fd] = events.get(fd, 0) | DangaReactor.EVENT_READ
            if kevent.filter == select.KQ_FILTER_WRITE:
                events[fd] = events.get(fd, 0) | DangaReactor.EVENT_WRITE
            if kevent.flags & select.KQ_EV_ERROR:
                events[fd] = events.get(fd, 0) | DangaReactor.EVENT_ERROR
        return events.items()


class _Select(object):
    """A simple, select()-based IOLoop implementation for non-Linux systems"""
    def __init__(self):
        self.read_fds = set()
        self.write_fds = set()
        self.error_fds = set()
        self.fd_sets = (self.read_fds, self.write_fds, self.error_fds)

    def register(self, fd, events):
        if events & DangaReactor.EVENT_READ: self.read_fds.add(fd)
        if events & DangaReactor.EVENT_WRITE: self.write_fds.add(fd)
        if events & DangaReactor.EVENT_ERROR: self.error_fds.add(fd)

    def modify(self, fd, events):
        self.unregister(fd)
        self.register(fd, events)

    def unregister(self, fd):
        self.read_fds.discard(fd)
        self.write_fds.discard(fd)
        self.error_fds.discard(fd)

    def poll(self, timeout):
        readable, writeable, errors = select.select(
                                        self.read_fds, 
                                        self.write_fds, 
                                        self.error_fds, 
                                        timeout)
        events = {}
        for fd in readable:
            events[fd] = events.get(fd, 0) | DangaReactor.EVENT_READ
        for fd in writeable:
            events[fd] = events.get(fd, 0) | DangaReactor.EVENT_WRITE
        for fd in errors:
            events[fd] = events.get(fd, 0) | DangaReactor.EVENT_ERROR
        return events.items()
    
    
# Choose a poll implementation. Use epoll if it is available, fall back to
# select() for non-Linux platforms
if hasattr(select, "epoll"):
    # Python 2.6+ on Linux
    _poll_obj = select.epoll
elif hasattr(select, "kqueue"):
    # Python 2.6+ on BSD or Mac
    _poll_obj= _KQueue
else:
    # All other systems
    _poll_obj= _Select
    
srv  = DangaReactor()
srv.reset()
srv.other_fds()
srv.run_timers()
sock = DangaSocket()