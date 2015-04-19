import argparse
import datetime
import logging
import os.path
import socket
import subprocess
import sys
import tempfile
import time

from rsyba import rsync

log = logging.getLogger(__name__)

def remote_path(archive):
    if ':' in archive:
        return archive.split(':', 1)[1]

    return archive

def main():
    argp = argparse.ArgumentParser(usage='%(prog)s [options] <archive> <local>...')
    argp.add_argument('--bwlimit', metavar='kbps', type=int, help='set transfer bandwidth limit')
    argp.add_argument('-n', '--dry-run', action='store_true', default=False, help='do not do any modifications')
    argp.add_argument('-f', '--filter', metavar='RULE', action='append', default=[], help='add source file filter rule')
    argp.add_argument('--hostname', metavar='FQDN', default=socket.gethostname(), help='override hostname [default %(default)s]')
    argp.add_argument('--max-file-size', metavar='INT[KMG]', default='1G', help='ignore files larger than this')
    argp.add_argument('--timeout', metavar='INT', type=int, default=12*60*60, help='set transfer time limit in seconds [default %(default)s]')
    argp.add_argument('archive', nargs=1, help='base URL of remote location')
    argp.add_argument('local', metavar='path[=tree]', nargs='+', help='local path with optional archive tree name')
    args = argp.parse_args()
    args.archive = args.archive[0]

    logging.basicConfig(stream=sys.stderr, level=logging.DEBUG, format='%(levelname).1s%(levelname).1s %(asctime)s %(message)s')
    ts = datetime.datetime.utcnow().strftime('%Y-%m-%dT%H-%M-%S.%f')
    start = time.time()
    nfiles = 0

    if os.isatty(sys.stdout.fileno()):
        def init_progress():
            print('Listing files...')
        def print_progress(ch):
            print('\033[1A[%d %s] %s %s\033[K' % (nfiles, datetime.timedelta(seconds=time.time() - start), ch.updates, ch.filename))
            sys.stdout.flush()
        def end_progress():
            print('\033[1A[%d %s] Done.\033[K' % (nfiles, datetime.timedelta(seconds=time.time() - start)))
    else:
        def init_progress():
            pass
        def print_progress(ch):
            log.debug('%s', ch)
        def end_progress():
            pass

    for pt in args.local:
        pt = pt.split('=', 1)
        if len(pt) == 1:
            (path, tree) = pt[0], os.path.basename(pt[0])
        else:
            (path, tree) = pt

        host_base = '/'.join([args.archive.rstrip('/'), 'upload', tree, args.hostname])

        log.debug('Starting upload for tree %r...', tree)
        while True:
            init_progress()
            try:
                it = rsync.run_iter(
                    '/'.join([host_base, ts, '']),
                    os.path.abspath(path) + os.sep,
                    archive=True,
                    bwlimit=args.bwlimit,
                    compress=True,
                    dry_run=args.dry_run,
                    fake_super=True,
                    filter=args.filter,
                    ignore_existing=True,
                    link_dest=remote_path('/'.join([host_base, 'latest', ''])),
                    max_size=args.max_file_size,
                    prune_empty_dirs=True,
                    safe_links=True,
                    temp_dir=remote_path('/'.join([host_base, 'tmp', ''])),
                    timeout=args.timeout,
                    gen_changes=rsync.FileChange(filename=True, size=True, updates=True, mtime=True))
                next_progress = time.time()
                for ch in it:
                    nfiles += 1
                    t = time.time()
                    if next_progress <= t:
                        print_progress(ch)
                        while next_progress <= t:
                            next_progress += 1
                break
            except subprocess.CalledProcessError as ex:
                now = time.time()
                if now - start >= args.timeout:
                    raise
            finally:
                end_progress()

            log.warning('Upload failed. Retrying in 5 s...')
            time.sleep(5)

        log.info('Finalizing upload of tree %r after %d files...', tree, nfiles)
        with tempfile.TemporaryDirectory(prefix='rsyba_tmp') as dpath:
            # TODO: latest-up is annoyingly concurrency unsafe. Add lock or don't overwrite a later one.
            os.symlink(ts, os.path.join(dpath, 'latest'))
            os.symlink(ts, os.path.join(dpath, ts + '.complete'))
            rsync.run(
                '/'.join([host_base, '']),
                os.path.join(dpath, 'latest'),
                os.path.join(dpath, ts + '.complete'),
                dry_run=args.dry_run,
                links=True,
                safe_links=True,
                temp_dir=remote_path('/'.join([host_base, 'tmp', ''])),
                timeout=60)

    log.info('All done after %s.', datetime.timedelta(seconds=time.time() - start))

if __name__ == '__main__':
    main()
