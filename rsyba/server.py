import argparse
import collections
import contextlib
import datetime
import getpass
import hashlib
import heapq
import logging
import multiprocessing as multip
import os.path
import socket
import stat
import subprocess as subp
import sys
import tempfile

log = logging.getLogger(__name__)

@contextlib.contextmanager
def replace_file(path, *args, **kwargs):
    with tempfile.NamedTemporaryFile(*args, delete=False, dir=os.path.dirname(path), **kwargs) as f:
        try:
            yield f
            os.rename(f.name, path)
        except:
            if os.path.exists(f.name):
                os.unlink(f.name)
            raise

class FileSystemArchive(object):
    def __init__(self, path):
        self.path = path
        
    def init(self):
        if not os.path.isdir(self.path):
            raise Exception('archive path does not exist: ' + self.path)
    
        os.makedirs(os.path.join(self.path, 'upload'), exist_ok=True)
        os.makedirs(os.path.join(self.path, 'download'), exist_ok=True)
        os.makedirs(os.path.join(self.path, 'tmp'), exist_ok=True)
        self.add_trees([], must_exist=False)

    def add_trees(self, trees, must_exist=True):
        old_trees = self.get_trees(must_exist=must_exist)

        with replace_file(os.path.join(self.path, 'trees.conf'), 'wt') as f:
            print('# Created on', datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%SZ'), file=f)
            for tree in old_trees + sorted(set(trees) - set(old_trees)):
                print(tree, file=f)

    def get_trees(self, must_exist=True):
        if must_exist or os.path.exists(os.path.join(self.path, 'trees.conf')):
            with open(os.path.join(self.path, 'trees.conf'), 'rt') as f:
                return [l.strip() for l in f.readlines() if not l.startswith('#') and l.strip()]
        else:
            return []

    def add_hosts(self, hosts, trees=None):
        ts = datetime.datetime.utcnow()
        ts = ts.strftime('%Y-%m-%dT%H-%M-%S.%f')

        for tree in trees:
            latest_up = None

            # Order matters for creation of latest, so make it consistent.
            for host in sorted(hosts):
                uhpath = os.path.join(self.path, 'upload', tree, host)
                if latest_up is None:
                    latest_up = self.get_latest_up_for_tree(tree)
                # The first makedirs should fail on existence as a sanity check.
                # The rest should not to avoid consistency issues.
                os.makedirs(os.path.join(uhpath, 'tmp'), exist_ok=False)
                if latest_up is None:
                    os.makedirs(os.path.join(uhpath, ts), exist_ok=True)
                    os.symlink(ts, os.path.join(uhpath, ts + '.complete'))
                    os.symlink(ts, os.path.join(uhpath, 'latest'))
                    latest_up = os.path.join(uhpath, ts)
                else:
                    os.symlink(os.path.relpath(latest_up, uhpath), os.path.join(uhpath, 'latest'))
                    
                dhpath = os.path.join(self.path, 'download', tree, host)
                os.makedirs(os.path.join(dhpath), exist_ok=True)
                os.symlink(os.path.relpath(latest_up, dhpath), os.path.join(dhpath, 'latest'))

    def get_host_path(self, tree, host):
        return os.path.join(self.path, 'upload', tree, host)
    
    def has_host(self, tree, host):
        return os.path.isdir(os.path.join(self.path, 'upload', tree, host))
    
    def get_latest_down(self, tree, host):
        hpath = os.path.join(self.path, 'download', tree, host)
        return os.path.normpath(os.path.join(hpath, os.readlink(os.path.join(hpath, 'latest'))))

    def get_latest_up(self, tree, host):
        hpath = os.path.join(self.path, 'upload', tree, host)
        return os.path.normpath(os.path.join(hpath, os.readlink(os.path.join(hpath, 'latest'))))

    def get_snapshots(self, tree, host):
        hpath = os.path.join(self.path, 'upload', tree, host)
        return [os.path.join(hpath, f.rsplit('.', 1)[0]) for f in sorted(os.listdir(hpath)) if f.endswith('.complete')]
    
    def get_latest_up_for_tree(self, tree):
        ret = None
        tpath = os.path.join(self.path, 'upload', tree)

        if not os.path.exists(tpath):
            return None
    
        for host in os.listdir(tpath):
            if host.startswith('.') or not self.has_host(tree, host):
                continue

            l = self.get_latest_up(tree, host)
            if ret is None or os.path.basename(l) > os.path.basename(ret):
                ret = l

        return ret

    def dedup_snapshots(self, tree, hosts):
        paths = set()
        
        for host in hosts:
            if not self.has_host(tree, host):
                continue

            latest_up = os.path.basename(self.get_latest_up(tree, host))
            hpath = self.get_host_path(tree, host)
            for ts in os.listdir(hpath):
                if not ts.endswith('.complete') or ts > latest_up:
                    continue
                
                paths.add(os.path.join(hpath, ts.rsplit('.', 1)[0]))

        with tempfile.TemporaryDirectory(dir=os.path.join(self.path, 'tmp')) as tmpd:
            for items in self._merge_file_iters(((self._list_files(p), p) for p in paths), key=lambda x: x[0]):
                if len(items) < 2:
                    continue

                # Split the items based on file content.
                # Save hash by inode to avoid re-hashing.
                inodes = {} # {inode: hash}
                files = collections.defaultdict(list) # {hash: [item]}
            
                for (name, inode, nlink), root in items:
                    h = inodes.get(inode)
                    if h is None:
                        h = self._get_file_hash(os.path.join(root, name))
                        inodes[inode] = h
                        
                    files[h].append((name, inode, nlink, root))

                # Find the best source for each group and hard link the others.
                for items in files.values():
                    source = max(items, key=lambda x: x[2])
                    for name, inode, nlink, root in items:
                        if inode == source[1]:
                            continue

                        tmpf = os.path.join(tmpd, 'link')
                        log.info('Replacing %s with %s...',
                                 os.path.join(root, name),
                                 os.path.join(source[3], source[0]))
                        os.link(os.path.join(source[3], source[0]), tmpf)
                        try:
                            os.rename(tmpf, os.path.join(root, name))
                        except:
                            os.unlink(tmpf)
                            raise

    def _get_file_hash(self, path):
        h = hashlib.new('sha256')
        with open(path, 'rb') as f:
            while True:
                d = f.read(65536)
                if not d: break
                h.update(d)
                
        return h.digest()
    
    def _merge_file_iters(self, iters, key=lambda x: x):
        heap = []
        for iter, root in iters:
            try:
                heap.append((next(iter), iter, root))
            except StopIteration:
                pass

        heapq.heapify(heap)
        while heap:
            value, iter, root = heapq.heappop(heap)
            try:
                heapq.heappush(heap, (next(iter), iter, root))
            except StopIteration:
                pass

            items = [(value, root)]
            while heap:
                cand, iter, root = heapq.heappop(heap)
                if key(cand) == key(value):
                    items.append((cand, root))
                    try:
                        heapq.heappush(heap, (next(iter), iter, root))
                    except StopIteration:
                        pass
                else:
                    heapq.heappush(heap, (cand, iter, root))
                    break

            yield items
    
    def _list_files(self, path):
        def do_list(q, path, root):
            def rec(path):
                for f in sorted(os.listdir(path)):
                    p = os.path.join(path, f)
                    st = os.stat(p, follow_symlinks=False)
                    if stat.S_ISDIR(st.st_mode):
                        rec(p)
                    elif stat.S_ISREG(st.st_mode) or stat.S_ISLNK(st.st_mode):
                        q.put((os.path.relpath(p, root), st.st_ino, st.st_nlink))
            try:
                rec(path)
            finally:
                q.put(None)
                q.close()

        q = multip.Queue(256)
        p = multip.Process(target=do_list, args=(q, path, path))
        p.start()
        try:
            while True:
                f = q.get()
                if f is None:
                    break

                yield f
        finally:
            p.join()

    def remove_snapshots(self, snapshots):
        snapshots = list(snapshots)
        
        # Ensure we don't remove a snapshot used as latest up.
        all_refs = set()
        for ss in snapshots:
            tpath = os.path.dirname(os.path.dirname(ss))
            for host in os.listdir(tpath):
                if host.startswith('.'):
                    continue
                all_refs.add(os.path.relpath(
                    os.readlink(os.path.join(tpath, host, 'latest')),
                    os.path.join(tpath, host)))

        def is_refd(ss):
            for ref in all_refs:
                absss = os.path.abspath(ss)
                if os.path.dirname(absss) == os.path.dirname(ref) and os.path.basename(ss) >= os.path.basename(ref):
                    return True
                
            return False
        
        for ss in snapshots:
            if is_refd(ss):
                continue
            
            log.info('Removing snapshot %s...', ss)
            # We assume rm(1) removes files in order of command line args.
            cmd = ['rm', '-fr', ss + '.complete', ss]
            with open(os.devnull, 'r') as devnull:
                p = subp.Popen(cmd, stdin=devnull)
            try:
                p.wait()
            except:
                p.terminate()
                raise
            finally:
                p.wait()
                if p.returncode:
                    raise subp.CalledProcessError(p.returncode, cmd)

    def filter_garbage_snapshots(self, snapshots, nsteps=1, base=2):
        """Compute the set of snapshots that can be pruned.

           This divides history from now into exponentially increasing buckets.
           The smallest bucket size is determined by the difference between
           the latest two snapshots. The function assigns each snapshot to
           the appropriate bucket and returns all but the latest per bucket. A
           consequence is that we never delete the latest two complete
           snapshots. A consequence of that is the function is also idempotent.

           We also return non-completed snapshots older than the first "nsteps"
           buckets.

           @param snapshots an iterable of snapshot paths.
           @return an iterable of snapshot paths.
        """
        prev_ts = None
        base_diff = None
        step = 0
        diff = None
        seen = False
        
        for ss in sorted(snapshots, reverse=True):
            ts = datetime.datetime.strptime(os.path.basename(ss) + ' UTC', '%Y-%m-%dT%H-%M-%S.%f %Z')
            if prev_ts is None:
                prev_ts = ts
                continue
            if base_diff is None:
                base_diff = prev_ts - ts
                prev_ts = ts
                diff = base_diff
                continue

            if ts >= prev_ts + diff:
                seen = False
                step += 1
                if step == nsteps:
                    steps = 0
                    diff *= base
                
            if not seen:
                seen = True
            else:
                yield ss

def create_archive(args):
    return FileSystemArchive(args.archive)

def args_init_archive(argp):
    argp.set_defaults(func=cmd_init_archive)

def cmd_init_archive(args):
    create_archive(args).init()
    
def args_add_sources(argp):
    argp.add_argument('--host', metavar='FQDN', action='append', default=[], help='host to add')
    argp.add_argument('--tree', metavar='STR', action='append', default=[], help='archive tree to add')
    argp.set_defaults(func=cmd_add_sources)

def cmd_add_sources(args):
    arch = create_archive(args)
    arch.add_trees(args.tree)
    if args.host:
        arch.add_hosts(args.host, arch.get_trees())

def args_dedup_snapshots(argp):
    argp.add_argument('--host', metavar='FQDN', action='append', default=[], help='host to dedup')
    argp.add_argument('--tree', metavar='STR', action='append', default=[], help='archive tree to dedup')
    argp.set_defaults(func=cmd_dedup_snapshots)

def cmd_dedup_snapshots(args):
    arch = create_archive(args)
    for tree in args.tree:
        arch.dedup_snapshots(tree, args.host)

def args_prune_snapshots(argp):
    argp.add_argument('-n', '--dry-run', action='store_true', default=False, help='only print what would be done')
    argp.add_argument('--host', metavar='FQDN', action='append', default=[], help='host to prune snapshots for')
    argp.add_argument('--tree', metavar='STR', action='append', default=[], help='archive tree to prune snapshots for')
    argp.set_defaults(func=cmd_prune_snapshots)

def cmd_prune_snapshots(args):
    arch = create_archive(args)
    for tree in args.tree:
        for host in args.host:
            snapshots = arch.filter_garbage_snapshots(arch.get_snapshots(tree, host))
            if args.dry_run:
                for ss in snapshots:
                    print('#', 'rm', '-fr', ss + '.complete', ss)
            else:
                arch.remove_snapshots(snapshots)

def args_rsync_server(argp):
    argp.add_argument('args', nargs=argparse.REMAINDER, help='rsync arguments to pass on')
    argp.set_defaults(func=cmd_rsync_server)

def cmd_rsync_server(args):
    if args.args and args.args[0] == '--':
        del args.args[0]

    # TODO: Check permissions
    # TODO: Create host directories as needed.
    
    p = subp.Popen(['rsync'] + args.args)
    try:
        p.wait()
    except:
        p.terminate()
        raise
    finally:
        p.wait()
        if p.returncode:
            sys.exit(p.returncode)

def main():
    argp = argparse.ArgumentParser()
    argp.add_argument('--archive', metavar='PATH', default='.', help='base path of archive location [default %(default)s]')
    subparsers = argp.add_subparsers(help='subcommands')
    args_init_archive(subparsers.add_parser('init', help='initialize an archive directory'))
    args_add_sources(subparsers.add_parser('add-sources', help='add sync sources (trees and hosts) to an archive'))
    args_dedup_snapshots(subparsers.add_parser('dedup-snapshots', help='traverse trees and deduplicate snapshot files'))
    #args_merge_files(subparsers.add_parser('merge-files', help='merge uploaded files into a download directory'))
    args_prune_snapshots(subparsers.add_parser('prune-snapshots', help='prune snapshots from archive trees'))
    #args_add_sources(subparsers.add_parser('remove-sources', help='remove sync sources (trees and hosts) from an archive'))
    args_rsync_server(subparsers.add_parser('rsync-server', help='run rsync in server mode (internal use only)'))
    args = argp.parse_args()

    logging.basicConfig(stream=sys.stderr)
    args.func(args)

if __name__ == '__main__':
    main()
