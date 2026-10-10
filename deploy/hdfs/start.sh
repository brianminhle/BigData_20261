#!/bin/sh
set -eu

case "${1:-}" in
  namenode)
    if [ ! -f /var/lib/hadoop/name/current/VERSION ]; then
      # A partial metadata directory needs inspection, never automatic reformatting.
      if [ -n "$(ls -A /var/lib/hadoop/name)" ]; then
        echo "NameNode metadata is nonempty without VERSION; refusing to format" >&2
        exit 1
      fi
      hdfs namenode -format -nonInteractive airtraffic-local
    fi
    exec hdfs namenode
    ;;
  datanode) exec hdfs datanode ;;
  *) exec "$@" ;;
esac
