#!/bin/bash
# vim:expandtab:shiftwidth=4:softtabstop=4:tabstop=4:

absolute_path() {
    (cd `dirname $1`; echo $PWD/`basename $1`)
}

init_test_env() {
        export SECTOR=`absolute_path $SECTOR`
        export SECTOR_HOME=`absolute_path $SECTOR`
        export TESTSUITE=`basename $0 .sh`
        export START_MASTER=${START_MASTER:-"${SECTOR}/master/start_master"}
        export START_SECURITY=${START_SECURITY:-"${SECTOR}/security/sserver"}
        export START_SLAVE=${START_SLAVE:-"${SECTOR}/slave/start_slave"}
        export MASTER_CONF=${MASTER_CONF:-"${SECTOR}/conf/master.conf"}
        export SAVE_PWD=${SAVE_PWD:-${SECTOR}/tests}
        export COPY=${COPY:-${SECTOR}/tools/sector_cp}
        export MKDIR=${MKDIR:-${SECTOR}/tools/sector_mkdir}
        export LS=${LS:-${SECTOR}/tools/sector_ls}
        export MV=${MV:-${SECTOR}/tools/sector_mv}
        export RM=${RM:-${SECTOR}/tools/sector_rm}
        export UPLOAD=${UPLOAD:-${SECTOR}/tools/sector_upload}
        export DOWNLOAD=${DOWNLOAD:-${SECTOR}/tools/sector_download}
        export SYSINFO=${SYSINFO:-${SECTOR}/tools/sector_sysinfo}
        export SECTOR_CMD=${SECTOR:-${SECTOR}/tests/send_dbg_cmd}
        export SECTOR_SHUTDOWN=${SECTOR_SHUTDOWN:-${SECTOR}/tools/sector_shutdown}
        export DSH=${DSH:-"ssh"}
        export SECTOR_HOST=${SECTOR_HOST:-`hostname`}
        export TEST_FAILED=false
        export I_MOUNTED=${I_MOUNTED:-"no"}
        export SLAVE_COUNT=${SLAVE_COUNT:-"3"}

        export SLAVE1_IP=${SLAVE1_IP:-"$SECTOR_HOST"}
        export SLAVE2_IP=${SLAVE2_IP:-"$SECTOR_HOST"}
        export SLAVE3_IP=${SLAVE3_IP:-"$SECTOR_HOST"}

        export SLAVE1_CONF=${SLAVE1_CONF:-"slave1_conf"}
        export SLAVE2_CONF=${SLAVE2_CONF:-"slave2_conf"}
        export SLAVE3_CONF=${SLAVE3_CONF:-"slave3_conf"}

        if ! echo $PATH | grep -q $SECTOR/tests; then
            export PATH=$PATH:$SECTOR/tests
        fi

        if ! echo $PATH | grep -q $SECTOR/lib; then
            export PATH=$PATH:$SECTOR/lib
        fi

        if ! echo $LD_LIBRARY_PATH | grep -q $SECTOR/lib; then
            export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$SECTOR/lib
        fi

        ONLY=${ONLY:-$*}
}

setup() {
         echo "start security server..."
         nohup $START_SECURITY > /dev/null &
         sleep 1
         echo "start master ...\n"
         nohup $START_MASTER > /dev/null &
         sleep 1
         for i in `seq 1 $SLAVE_COUNT`; do
                local SLAVE_CONF=SLAVE${i}_CONF
                echo "start slave $i ...\n"
                ${START_SLAVE} ${!SLAVE_CONF} & > /dev/null &
         done

         #wait master to update stat information
         sleep 2
         REAL_SLAVES=`${SYSINFO} | grep Slave | awk '{print $6}'`
         if [ ${REAL_SLAVES} -ne ${SLAVE_COUNT} ]; then
            error "slave only setup ${REAL_SLAVES}, but require ${SLAVE_COUNT}"
         fi
}

check_and_setup_sector() {
         if ! ps aux | grep start_master | grep -v grep 1>&2 > /dev/null; then
                setup
                export I_MOUNTED=yes
         fi 
}

cleanup() {
         set -vx
         local REAL_SLAVES=`${SYSINFO} | grep Slave | awk '{print $6}'`
         for i in `seq 1 $REAL_SLAVES`; do
                slave_id=`${SYSINFO} | tail -n $i | head -n 1 | awk '{print $1}'` 
                $SECTOR_SHUTDOWN -i $slave_id 
         done
         killall -9 sserver
         killall -9 start_master
}

check_and_cleanup_sector() {
         if ps aux | grep start_master | grep -v grep; then
                if [ "$I_MOUNTED" = "yes" ]; then
                        cleanup
                fi 
         fi 
}

read_only() {
         local readonly_slave=$1
         ${SECTOR_CMD} $readonly_slave -c 9901 
}

fail() {
         local failed_id=$1
         ${SECTOR_SHUTDOWN} $failed_id 
}

get_slave_address() {
        local slave_id=$1
        local REAL_SLAVES=`${SYSINFO} | grep Slave | awk '{print $6}'`
        $SYSINFO | tail -n $((REAL_SLAVES - slave_id)) | head -n 1 | awk '{print $2}'
}

get_slave_id() {
        local slave_id=$1
        local REAL_SLAVES=`${SYSINFO} | grep Slave | awk '{print $6}'`
        $SYSINFO | tail -n $((REAL_SLAVES - slave_id)) | head -n 1 | awk '{print $1}'
}

build_test_filter() {
        [ "$ONLY" ] && echo "only running test `echo $ONLY`"
        for O in $ONLY; do
               eval ONLY_${O}=true 
        done
}

error_no_exit() {
        echo $@
        TEST_FAILED=true
}

error() {
        error_no_exit
        exit 1
}

basetest() {
        if [[ $1 = [a-z]* ]]; then
                echo $1
        else
                echo ${1%%[a-z]*}
        fi
}

pass() {
        $TEST_FAILED && echo -n "FAIL " || echo -n "PASS "
        echo $@
}

run_one() {
        local testnum=$1
        local message=$2
        tfile=f${testnum}
        export tdir=d${base}
        export TESTNAME=test_$testnum

        local BEFORE=`date +%s`
        echo "== test $testnum: $message == `date +%H:%M:%S` ($BEFORE)"
        TEST_FAILED=false
        test_${testnum} || echo "test_$testnum failed with $?"

        pass "($((`date +%s` - $BEFORE))s)"
        TEST_FAILED=false
        cd $SAVED_PWD 
        unset TESTNAME
        unset tdir
        unset tfile
        unset base
        return $?
}

run_test() {
    export base=`basetest $1`
    if [ ! -z "$ONLY" ]; then
        testname=ONLY_$1
        if [ ${!testname}x != x ]; then
            [ "$LAST_SKIPPED" ] && echo "" && LAST_SKIPPED=
            run_one $1 "$2"
            return $?
        fi
        testname=ONLY_$base
        if [ ${!testname}x != x ]; then
            [ "$LAST_SKIPPED" ] && echo "" && LAST_SKIPPED=
            run_one $1 "$2"
            return $?
        fi
        LAST_SKIPPED="y"
        echo -n "."
        return 0
    fi

    LAST_SKIPPED=
    run_one $1 "$2"

    return $?
}
