#!/bin/bash -e

teardown() {
    [ -d "$d" ] && rm -fr "$d"
}

trap 'teardown' EXIT

setup() {
    d="$(mktemp -d --tmpdir rsyba_test.XXXXXXXXXX)"
    mkdir "$d/archive"
    python3 -m rsyba.server --archive="$d/archive" init
    python3 -m rsyba.server --archive="$d/archive" add-sources --host=host1 --tree=a --tree=b
    python3 -m rsyba.server --archive="$d/archive" add-sources --host=host2 --tree=a

    mkdir -p "$d/local"/host1/{a,b}
    echo aA >"$d/local/host1/a/A"
    echo aB >"$d/local/host1/a/B"
    echo aC >"$d/local/host1/a/C"
    echo bA >"$d/local/host1/b/A"
    
    mkdir -p "$d/local"/host2/a
    echo aA >"$d/local/host2/a/A"
    echo aD >"$d/local/host2/a/D"
    echo aE >"$d/local/host2/a/E"
}

listfiles() {
    find "$1" -ls | sed -e "s:$1:<tmp>:g" | sort -k11,11
}

test_two_trees() {
    python3 -m rsyba.client --hostname=host1 "$d/archive" "$d/local/host1/a" "$d/local/host1/b"
}

test_two_snapshots() {
    python3 -m rsyba.client --hostname=host1 "$d/archive" "$d/local/host1/a"
    sleep 0.01
    python3 -m rsyba.client --hostname=host1 "$d/archive" "$d/local/host1/a"
}

test_prune_snapshots() {
    python3 -m rsyba.client --hostname=host1 "$d/archive" "$d/local/host1/a"
    sleep 0.01
    python3 -m rsyba.client --hostname=host1 "$d/archive" "$d/local/host1/a"
    sleep 0.01
    python3 -m rsyba.client --hostname=host1 "$d/archive" "$d/local/host1/a"
    sleep 0.01
    python3 -m rsyba.client --hostname=host1 "$d/archive" "$d/local/host1/a"
    
    python3 -m rsyba.server --archive="$d/archive" prune-snapshots --host=host1 --tree=a
}

test_dedup_snapshots() {
    python3 -m rsyba.client --hostname=host1 "$d/archive" "$d/local/host1/a"
    python3 -m rsyba.client --hostname=host2 "$d/archive" "$d/local/host2/a"
    python3 -m rsyba.server --archive="$d/archive" dedup-snapshots --host=host1 --host=host2 --tree=a
}

if [ $# -eq 0 ]; then
    tests=( $(declare -F | sed -e 's:^declare -f :: p ; d' | grep '^test_') )
else
    tests=( "$@" )
fi

for tst in "${tests[@]}"; do
    echo "Starting $tst..."
    setup
    "$tst"
    listfiles "$d"
    teardown
done
