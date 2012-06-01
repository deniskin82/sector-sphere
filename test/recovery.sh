#!/bin/bash
# vim:expandtab:shiftwidth=4:softtabstop=4:tabstop=4:

TMP=${TMP:-/tmp}
SECTOR=$(cd $(dirname $0)/..; echo $PWD)
export PATH=$PATH:/sbin

. $SECTOR/tests/test-framework.sh
init_test_env $@

DIR=${DIR:-$MOUNT}

build_test_filter

if [ "$ONLY" == "cleanup" ]; then
        check_and_cleanup_sector
        exit 0
fi

check_and_setup_sector

if [ "$ONLY" == "setup" ]; then
        exit 0
fi

test_0() {
        local slave_address
        echo test0 > $TMP/$tfile
        $UPLOAD $TMP/$tfile /
        kill -9 start_slave
        for i in `seq 1 $SLAVE_COUNT`; do
                local SLAVE_NODE=SLAVE${i}_IP
                ${START_SLAVE} $SECTOR & > /dev/null &
        done
        $LS /$tfile | grep $tfile && error "failed: did not copy $tdir"
}
run_test 0 "restart slave and validate the file"

test_1() {
        local slave_address
        echo test0 > $TMP/$tfile
        slave_address=`get_slave_address 1`
        slave_id=`get_slave_id 1`
        readonly $slave_address 
        $UPLOAD $TMP/$tfile /
        fail $slave_id
        #FIXME restart this slave and check whether the recovery daemon create on this file 
        $LS /$tfile | grep $tfile && error "failed: did not copy $tdir"
}
run_test 1 "make sure the recovery daemon can replicate the file during slave failove "

cleanup

