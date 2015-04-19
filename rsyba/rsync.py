import datetime
import logging
import os
import subprocess
import sys

log = logging.getLogger(__name__)

EXIT_SUCCESS = 0
EXIT_USAGE = 1
EXIT_PROTOCOL = 2
EXIT_SELECT = 3
EXIT_NOTSUPPORTED = 4
EXIT_START = 5
EXIT_LOG = 6
EXIT_SOCKET = 10
EXIT_FILE = 11
EXIT_DATA = 12
EXIT_DIAG = 13
EXIT_IPC = 14
EXIT_INT = 20
EXIT_WAIT = 21
EXIT_ALLOC = 22
EXIT_PARTIAL = 23
EXIT_VANISHED = 24
EXIT_MAX_DELETE = 25
EXIT_IO_TIMEOUT = 30
EXIT_CONN_TIMEOUT = 35

def parse_ts(s):
    return datetime.datetime.strptime(s, '%Y/%m/%d-%H:%M:%S')

class FileUpdates(object):
    def __init__(self, s):
        self.s = s

    def __repr__(self):
        return '%s(%r)' % (self.__class__.__name__, self.s)

    def __str__(self):
        return self.s

    @property
    def update_type(self): return self.s[0]
    @property
    def file_type(self): return self.s[1]
    @property
    def checksum(self): return self.s[2].replace('c', '-')
    @property
    def size(self): return self.s[3].replace('s', '-')
    @property
    def mtime(self): return self.s[4].replace('t', '-') # TODO: Handle T
    @property
    def perms(self): return self.s[5].replace('p', '-')
    @property
    def owner(self): return self.s[6].replace('o', '-')
    @property
    def group(self): return self.s[7].replace('g', '-')
    @property
    def acl(self): return self.s[9].replace('a', '-')
    @property
    def xattr(self): return self.s[10].replace('x', '-')

class FileChange(object):
    __fields = [
        'transferred',
        'posix_perms',
        'filename',
        'updates',
        'size',
        'mtime',
        'op',
        'time',
        'uid',
        'gid']

    def __init__(self, **kwargs):
        for k in self.__fields:
            setattr(self, k, kwargs.get(k, None))

    def __repr__(self):
        return '%s(%s)' % (
            self.__class__.__name__,
            ', '.join('%s=%r' % (f, getattr(self, f))
                      for f in self.__fields
                      if getattr(self, f) is not None))

RSYNC = os.environ.get('RSYBA_RSYNC', 'rsync')
CHANGE_PREFIX = '$change'

def run(*args, gen_changes=None, **kwargs):
    if gen_changes is not None:
        raise TypeError('gen_changes not supported with run(); use run_iter()')

    for _ in run_iter(*args, **kwargs):
        pass

def run_iter(dest, *srcs, rsync_bin=RSYNC, gen_changes=None, **kwargs):
    kwargs.setdefault('motd', False)
    #kwargs.setdefault('rsync_path', 'rsyba-server --')

    out_format = []
    out_fields = []
    def add_out(fmt, field, parser=lambda x: x):
        if gen_changes is True or getattr(gen_changes, field):
            out_format.append(fmt)
            out_fields.append((field, parser))

    if gen_changes:
        out_format.append(CHANGE_PREFIX)
        add_out('%b', 'transferred', int)
        add_out('%B', 'posix_perms')
        add_out('%i', 'updates', FileUpdates)
        add_out('%l', 'size', int)
        add_out('%M', 'mtime', parse_ts)
        add_out('%o', 'op')
        add_out('%t', 'time', parse_ts)
        add_out('%U', 'uid', int)
        add_out('%G', 'gid', int)
        # Always last:
        add_out('%f', 'filename')

        kwargs['out_format'] = '\t'.join(out_format)
    else:
        kwargs.setdefault('quiet', True)

    opts = []
    for optl in [_option(k, v) for (k, v) in sorted(kwargs.items())]:
        opts.extend(optl)

    cmd = [rsync_bin] + opts + list(srcs) + [dest]
    p = subprocess.Popen(cmd, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE if gen_changes else None)

    try:
        if gen_changes:
            for item in p.stdout:
                item = str(item, 'utf-8')
                if not item.startswith(CHANGE_PREFIX + '\t'):
                    continue

                parts = item.rstrip().split('\t', len(out_fields) - 1 + 1)[1:]
                yield FileChange(**{field: func(s) for s, (field, func) in zip(parts, out_fields)})

            p.stdout.close()
        else:
            # Wait here to catch exceptions and terminate() if that happens.
            p.wait()
    except:
        p.terminate()
        raise
    finally:
        p.wait()
        if p.returncode:
            # Don't raise CPE if rsync terminated because of SIGINT and we
            # already have an active exception.
            if p.returncode != EXIT_INT or sys.exc_info()[0] is None:
                raise subprocess.CalledProcessError(p.returncode, cmd)

def _option(k, v):
    k = k.replace('_', '-')

    if v is True:
        return ['--' + k]
    elif v is False:
        if k in ('dry-run',):
            return []

        return ['--no-' + k]
    elif isinstance(v, list):
        return [_option(k, vv) for vv in v]
    elif v is None:
        return []
    else:
        return ['--%s=%s' % (k, v)]
