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
        echo test0 > $TMP/$tfile
        $RM /$tfile 2>/dev/null 
        $UPLOAD $TMP/$tfile /
        $LS / | grep $tfile || error "failed: did not find $tfile"
        $RM /$tfile 
        $LS / | grep $tfile && error "failed: did not rm $tfile"
        rm -rf $TMP/$tfile
}
run_test 0 "upload .../file ; ls..../file rm .../file =============="

test_1() {
        $RM --f /$tdir 2>/dev/null
        $MKDIR $tdir
        $LS / | grep $tdir || error "failed: did not find $tdir"
        $RM /$tdir 
        $LS / | grep $tdir && error "failed: did not rm $tdir"
}
run_test 1 "mkdir .../dir ; ls..../dir rm .../dir =============="

test_2() {
        echo test0 > $TMP/$tfile
        $RM /$tfile  2>/dev/null 
        $RM /${tfile}_1  2>/dev/null 
        $RM --f /$tdir  2>/dev/null 
        $UPLOAD $TMP/$tfile /
        $MKDIR $tdir
        $MV /$tfile /${tfile}_1 || error "failed: mv failed "
        $LS / | grep ${tfile}_1 || error "failed: did not mv ${tfile} to ${tfile}_1"
        rm -rf $TMP/$tfile
}
run_test 2 "upload .../file ; mv..file /dir rm .../dir =============="

test_3() {
        echo test0 > $TMP/$tfile
        $RM /$tfile  2>/dev/null 
        $RM /${tfile}_1 2>/dev/null
        $UPLOAD $TMP/$tfile /
        $MV /$tfile /${tfile}_1
        $DOWNLOAD /${tfile}_1 $TMP/
        diff $TMP/$tfile $TMP/${tfile}_1 || error "download got different file!"
        rm -rf $TMP/$tfile
        rm -rf $TMP/${tfile}_1
}
run_test 3 "upload .../file ; download .../file compare =============="

test_4() {
        echo test0 > $TMP/$tfile
        $RM /$tfile 2>/dev/null
        $RM  --f /${tdir} 2>/dev/null
        $UPLOAD $TMP/$tfile /
        $MKDIR /$tdir
        $COPY /$tfile /${tdir}
        $LS /$tdir | grep $tfile && error "failed: did not copy $tdir"
        $DOWNLOAD /$tdir/$tfile $TMP/${tfile}_download
        diff $TMP/$tfile $TMP/${tfile}_download || error "download got different file!"
        rm -rf $TMP/${tfile}_download
}
run_test 4 "upload ...tfile, copy ...tfile /tdir, download tdir/tfile tfile_download, diff tfile_download tfile..."

check_and_cleanup_sector

