#!/bin/bash

set -o errexit
[ -n "$DEBUG" ] && set -x

usage()
{
    echo "Usage : $0 COMMAND [-q]" 1>&2
    echo "Commands:"
    echo "        run       Runs a fresh database server"
    echo "        url       Returns an url that can be used to connect to the database"
    echo "        test      Tests the connection to the database"
    echo "        console   Opens an SQL console"
    echo "        restore|import|console|sql|reset-credentials|connection|version|create-database|drop-database> [-q]" 1>&2
    echo "        $0 <backup|export> [-q] > data.tar.gz" 1>&2
    exit 1
}

if [ $# -lt 1 ]; then
    usage
fi

COMMAND=$1
shift
if [ "$1" = "-q" ]; then
    QUIET=$1
    shift
fi

. /usr/local/share/dokdb/common.bash

# forked from https://github.com/synthomat/dockerfiles
VERSION=9.3
PGDATA=/var/lib/postgresql/$VERSION/main
PGCFG=/var/lib/postgresql/etc/$VERSION/main
PGBIN=/usr/lib/postgresql/$VERSION/bin/postgres

execute_single() {
  if [ -n "$2" ]; then
    local DB="$1"
    shift
  fi
  $PGBIN --single  -c config_file=$PGCFG/postgresql.conf $DB <<< "$1"
}

execute_psql() {
  if [ -z "$1" ]; then
    echo "Missing database connection.."
    exit -1
  fi
  parse_url "$1" NOW
  shift
  test -n "$NOW_PORT" || NOW_PORT="$DATABASE_PORT"
  test -n "$NOW_USER" || NOW_PORT="$DATABASE_USER"
  test  -z "$ECHO" || \
    echo PGPASSWORD="$NOW_PASSWORD" psql -v ON_ERROR_STOP=on -h "$NOW_HOST" \
      -p "$NOW_PORT" -U "$NOW_USER" "$NOW_NAME" -q -c "$@" 1>&2
  PGPASSWORD="$NOW_PASSWORD" psql -v ON_ERROR_STOP=on -h "$NOW_HOST" \
    -p "$NOW_PORT" -U "$NOW_USER" "$NOW_NAME" -q -c "$@"
  local ret=$?
  test  -z "$ECHO" || echo "Exit: $ret" 1>&2
  if [ $ret -ne 0 ]; then
    exit $ret
  fi
}

execute_sql() {
  local url="$1"
  shift
  execute_psql "$url" -q  -c "$@"
}

execute_sql_to_csv() {
  local url="$1"
  local sql="$2"
  shift
  shift
  execute_sql "$url" "COPY ($sql) TO STDOUT with CSV HEADER" "$@"
}

execute_admin_psql() {
  local now_url
  parse_url "$DATABASE_URL" NOW
  NOW_NAME=postgres
  now_url="$(make_url_from_env NOW)"
  execute_sql "$now_url" "$@"
}

has_database() {
  $EXECUTE "SELECT datname FROM pg_database where datname='$1'" | grep -q $1
}

has_user() {
  $EXECUTE "SELECT usename FROM pg_user WHERE usename='$1'" | grep -q $1
}

create_database() {
  local pwd="$(echo "$DATABASE_PASSWORD" | sed s/"'"/"''"/g)"
  if ! has_user "$DATABASE_USER"; then
    message "Creating user: $DATABASE_USER"
    indent_cmd $EXECUTE "CREATE ROLE \"$DATABASE_USER\" WITH LOGIN PASSWORD '$pwd' VALID UNTIL 'infinity'"
    message
  else
    message "Update user: $DATABASE_USER"
    indent_cmd $EXECUTE "ALTER USER \"$DATABASE_USER\" WITH PASSWORD '$pwd';"
  fi

  if ! has_database "$DATABASE_NAME"; then
    message "Creating database: $DATABASE_NAME"
    indent_cmd $EXECUTE "CREATE DATABASE \"$DATABASE_NAME\" OWNER=\"$DATABASE_USER\""
    message
  fi
}

cleanup_cluster() {
  message "Cleaning up cluster..."
  indent_cmd rm -rf $PGCFG
  indent_cmd rm -rf $PGDATA
  message "Cleaning up done..."
}

ensure_cluster() {
  sudo /usr/local/bin/own-volume
  # Ensure that the requsted locale is available
  if [ -z "$(locale -a | grep $DATABASE_LOCALE)" ]; then
    indent_cmd sudo /usr/sbin/locale-gen $DATABASE_LOCALE
  fi
  if [ ! -d "$PGDATA" ]; then

    # Actually create the cluster
    trap cleanup_cluster INT TERM EXIT
    echo $DATABASE_ADMIN_PASSWORD > /tmp/pwroot
    indent_cmd /usr/bin/pg_createcluster -u postgres --locale=$DATABASE_LOCALE $VERSION main \
      -- -U $DATABASE_ADMIN_USER --pwfile=/tmp/pwroot
    indent_cmd rm /tmp/pwroot

    # Adjust PostgreSQL configuration so that remote connections to the
    # database are possible. 
    echo 'host all all 0.0.0.0/0 md5' >> $PGCFG/pg_hba.conf
    echo "listen_addresses='*'" >> $PGCFG/postgresql.conf
    trap - SIGTERM
  else
    local pwd="$(echo "$DATABASE_ADMIN_PASSWORD" | sed s/"'"/"''"/g)"
    message "Update user: $DATABASE_ADMIN_USER"
    indent_cmd execute_single postgres "ALTER USER \"$DATABASE_ADMIN_USER\" WITH PASSWORD '$pwd';"
  fi
  if [ "$DATABASE_EXTENSIONS" != "optional" ]; then
    message "Creating extensions: $DATABASE_EXTENSIONS"
    for k in $(echo $DATABASE_EXTENSIONS | tr ',' '\n'); do
      indent_cmd execute_single template1 "CREATE EXTENSION IF NOT EXISTS \"$k\""
      message
    done
  fi
}

remove_pid() {
  trap - EXIT SIGINT SIGTERM 
  rm -f $PGDATA/postmaster.pid
  exit 0
}

case "$COMMAND" in
    run)
      trap remove_pid EXIT SIGINT SIGTERM
      ensure_cluster

      if [ "$DATABASE_SERVER_ONLY" != "true" ]; then
        EXECUTE=execute_single
        create_database
      fi
      exec $PGBIN -c config_file=$PGCFG/postgresql.conf
      ;;
    create-database)
      set_dokdb_database_url --admin
      EXECUTE=execute_admin_psql
      create_database
      if [ "$1" == "-p" -o "$1" == "--proxy" ]; then
        parse_url "$DATABASE_URL" NOW
        message "Starting proxy"
        socat TCP4-LISTEN:$DATABASE_PORT,fork,reuseaddr TCP4:$NOW_HOST:$NOW_PORT
      fi
      ;;
    drop-database)
      set_dokdb_database_url --admin
      EXECUTE=execute_admin_psql
      if has_database "$DATABASE_NAME"; then
        message "Dropping database: $DATABASE_NAME"
        indent_cmd execute_admin_psql "DROP DATABASE \"$DATABASE_NAME\""
      else
        message "No such database: $DATABASE_NAME"
        exit 1
      fi
      if has_user "$DATABASE_USER"; then
        message "Dropping user: $DATABASE_USER"
        indent_cmd execute_admin_psql "DROP USER \"$DATABASE_USER\""
      else
        message "No such user: $DATABASE_USER"
        exit 2
      fi
      ;;
    list-databases)
      set_dokdb_database_url --admin
      execute_admin_psql "SELECT datname FROM pg_database ORDER BY datname" -t -P format=unaligned
      ;;
    test)
      set_dokdb_database_url "$1"
      cmd execute_sql "$DATABASE_URL" "SELECT 1"
      ;;
    sql)
      set_dokdb_database_url "$1"
      if [ "$1" == "-a" -o "$1" == "--admin" ]; then
        shift
      fi
      execute_sql_to_csv "$DATABASE_URL" "$1"
      ;;
    console)
      set_dokdb_database_url "$1"
      execute_psql "$DATABASE_URL"
      ;;
    self-test-setup)
      set_dokdb_database_url
      cmd execute_sql "$DATABASE_URL" "CREATE TABLE Standard (id INTEGER, name VARCHAR(200), PRIMARY KEY(id))"
      cmd execute_sql "$DATABASE_URL" "INSERT INTO Standard (id, name) VALUES (1, 'Brian')"
      cmd execute_sql "$DATABASE_URL" "INSERT INTO Standard (id, name) VALUES (2, 'Søren')"
      cmd execute_sql "$DATABASE_URL" "INSERT INTO Standard (id, name) VALUES (3, 'Jagadish')"
      ;;
    self-test)
      set_dokdb_database_url
      message_n "Testing preloaded data length... "
      result="$(execute_sql "$DATABASE_URL" "SELECT count(id) FROM Standard" -t -P format=unaligned 2>&1)"
      message "$result"
      test "$result" == "3" || exit 1

      message_n "Testing preloaded data id column... "
      result="$(execute_sql "$DATABASE_URL" "SELECT id FROM Standard ORDER BY id" -t -P format=unaligned | xargs 2>&1)"
      message "$result"
      test "$result" == "1 2 3" || exit 2

      message_n "Testing preloaded data name column... "
      result="$(execute_sql "$DATABASE_URL" "SELECT name FROM Standard ORDER BY name" -t -P format=unaligned | xargs 2>&1)"
      message "$result"
      test "$result" == "Brian Jagadish Søren" || exit 3

      message_n "Testing data insert... "
      execute_sql "$DATABASE_URL" "INSERT INTO Standard (id, name) VALUES (4, 'Jacob')"
      result="$(execute_sql "$DATABASE_URL" "SELECT count(id) FROM Standard" -t -P format=unaligned 2>&1)"
      message "$result"
      test "$result" == "4" || exit 4
      ;;
    url)
      set_dokdb_database_url "$1"
      echo $DATABASE_URL
      ;;
    export)
      if [[ "$1" == "-" ]]; then
        export_file="$1"
      else
        export_file=/tmp/database.dump
        if [[ "$1" ]]; then
          put_url="$1"
        fi
        file_arg="--file $export_file"
      fi
      if [ -z "$QUIET" ]; then
        output="--verbose"
      fi
      set_dokdb_database_url
      parse_url "$DATABASE_URL" NOW
      PGPASSWORD="$NOW_PASSWORD" indent_cmd pg_dump -Fc $output -h "$NOW_HOST" -p "$NOW_PORT" -U "$NOW_USER" $EXPORT_OPTS $file_arg "$NOW_NAME"
      if [[ "$export_file" != "-" ]]; then
        export_size=$(du -Sh "$export_file" | cut -f1)
        message "Exported database size is $export_size"

        if [[ $put_url ]]; then
          curl -0 -s -o /dev/null -X PUT -T $export_file "$put_url" 
        fi
      fi
      ;;
    import)
      import_file=/tmp/database.dump
      if [[ "$1" == "-" ]]; then
        cat > $import_file
      else
        curl -s "$1" -o $import_file
      fi
      pg_restore -l $import_file | grep -v 'COMMENT - EXTENSION plpgsql' > $import_file.list
      test -n "$IMPORT_OPTS" || IMPORT_OPTS="-Fc --no-owner --no-acl --verbose -L $import_file.list"
      import_size=$(du -Sh "$import_file" | cut -f1)
      message "Importing database size of $import_size"
      set_dokdb_database_url
      parse_url "$DATABASE_URL" NOW
      PGPASSWORD="$NOW_PASSWORD" indent_cmd pg_restore -h "$NOW_HOST" -p "$NOW_PORT" -U "$NOW_USER" -d "$NOW_NAME" $IMPORT_OPTS "$import_file"
      ;;
    extensions)
      echo "import/export"
      echo "create-database"
      echo "self-test"
      ;;
    bash)
      bash "$@"
      ;;
    *) usage ;;
esac
