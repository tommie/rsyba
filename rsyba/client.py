import argparse
import datetime
import logging
import os.path
import socket
import sys
import tempfile

from rsyba import rsync

log = logging.getLogger(__name__)

def main():
    argp = argparse.ArgumentParser(usage='%(prog)s [options] <archive> <local>...')
    argp.add_argument('--bwlimit', metavar='kbps', type=int, help='set transfer bandwidth limit')
    argp.add_argument('-n', '--dry-run', action='store_true', default=False, help='do not do any modifications')
    argp.add_argument('-f', '--filter', metavar='RULE', action='append', default=[], help='add source file filter rule')
    argp.add_argument('--hostname', metavar='FQDN', default=socket.getfqdn(), help='override hostname [default %(default)s]')
    argp.add_argument('--max_file_size', metavar='INT[KMG]', default='500M', help='ignore files larger than this')
    argp.add_argument('archive', nargs=1, help='base URL of remote location')
    argp.add_argument('local', nargs='+', help='local path with optional archive tree name')
    args = argp.parse_args()
    args.archive = args.archive[0]

    logging.basicConfig(stream=sys.stderr, level=logging.DEBUG)
    ts = datetime.datetime.utcnow()
    ts = ts.strftime('%Y-%m-%dT%H-%M-%S.%f')

    for pt in args.local:
        pt = pt.split('=', 1)
        if len(pt) == 1:
            (path, tree) = pt[0], os.path.basename(pt[0])
        else:
            (path, tree) = pt

        host_base = '/'.join([args.archive.rstrip('/'), 'upload', tree, args.hostname])

        log.info('rsync starting...')
        rsync.run(
            '/'.join([host_base, ts, '']),
            os.path.abspath(path) + os.sep,
            archive=True,
            belimit=args.bwlimit,
            compress=True,
            dry_run=args.dry_run,
            fake_super=True,
            filter=args.filter,
            ignore_existing=True,
            link_dest='/'.join([host_base, 'latest', '']),
            max_size=args.max_file_size,
            prune_empty_dirs=True,
            safe_links=True,
            temp_dir='/'.join([host_base, 'tmp', '']),
            timeout=60)

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
                temp_dir='/'.join([host_base, 'tmp', '']),
                timeout=60)

if __name__ == '__main__':
    main()
