import logging
import os
import subprocess

log = logging.getLogger(__name__)

RSYNC = os.environ.get('RSYBA_RSYNC', 'rsync')

def run(dest, *srcs, rsync_bin=RSYNC, **kwargs):
    kwargs.setdefault('motd', False)
    kwargs.setdefault('quiet', True)
    kwargs.setdefault('rsync_path', 'rsyba-server --')

    opts = []
    for optl in [_option(k, v) for (k, v) in sorted(kwargs.items())]:
        opts.extend(optl)

    cmd = [RSYNC] + opts + list(srcs) + [dest]
    with open(os.devnull, 'r') as devnull:
        p = subprocess.Popen(cmd, stdin=devnull)
        
    try:
        p.communicate()
    except:
        p.terminate()
        raise
    finally:
        p.wait()
        if p.returncode:
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
    elif v:
        return ['--%s=%s' % (k, v)]
    else:
        return []
