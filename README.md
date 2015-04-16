rsyba - Rsync Backups
=====================
Tommie Gannert

This is a research project, and is not meant for production use.

The idea is to have a central archive of backup snapshots for workstations.
Each workstation uploads to a new snapshot, using the rsync --hard-link
feature to use the previous snapshot as a base. Periodically a job is run
to garbage collect old snapshots and to ensure hard links are used consistently
for all hosts.

The archive is divided into _trees_. Each tree is a shared folder. When
multiple hosts use the same tree, they upload to separate snapshot directories
and a periodic job merges the latest snapshot directories to form download
snapshots. Each host then downloads its download snapshot to get files
available on other hosts. Collisions are managed by a pluggable strategy that
takes the latest version of the file from all hosts, and the hostname to
generate the download snapshot for. It produces a set of new files in the
snapshot directory. Further improvement is using a union file system to handle
creation of download snapshots.

One example of a strategy is simply taking the latest file (by snapshot
timestamp or mtime). The other files are made available through a web
interface. Another example is adding a host suffix to all files but the host's
own and let the host download all the files.
