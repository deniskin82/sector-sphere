Written by Sergey 01/10/2012
This document show differences in stable branch to documentation. It is recommended to read this file before using Sector from Stable branch. It include information specific to stable branch and not reflected in documention.

Goal of Stable branch is to get stable and bug-free implementation of Sector, with reliability and help of Sector administration in mind.

Stable branch fixes set of bugs. It also introduced some additional functionality, as well as change to configuration parameters.

Stable branch work exclusively around linux, most probably changes will needed
to port to oter platforms.

Changes to tools parameters

sector_download [-smart] "-smart"  will resume download of partially downloaded.
sector-sysinfo [-d|-a] "-a" will show address of slave, "-d" will show currently running transactions, users who logged in, replication in process.
sector_ls [-r|-a] "-r" will show additional 2 columns - current nubmer of replicas and configured number of replicas, -a additionally will show list of replicas. Both -r|-a] will show total size fo files and total size of replicas in directory.

Changes to configuration parameters:

In .conf file, any combination of space/tab in front of parameter value will do a job.

master.conf:
PROCESS_THREADS - number of process threads run by master. 1 will be single threaded operation. Probably reasonable limits would be between 4 and 64.

replica.conf:
REPLICATION_MAX_TRANS
 Maximum number of replication running in parallel. Hardcoded limit is twice the number of currently connected slaves. 0 means 2x number of slaves (only during time of initial read or change of replcia.conf). -1 will turn replication off.
master log contains messages about max transaction change. replica.conf is reread if it was changed (based on timestamp) and full directory scan is run, so changes to replica.conf can be done on a fly.
Despite name of this parameters, it is max number of replication running in parallel. Each replciation produces 2 transactions, so number of transactions will be twice more.


REPLICATION_START_DELAY
Number of seconds to wait before first full scan kick off, to find under/overreplicated files. All slaves should be able to connect to master during this time, to avoid unnecesary replications.

REPLICATION_FULL_SCAN_DELAY
 Period in sec between full scans

DISK_BALANCE_AGGRESSIVENESS
When slave became full, Sector will move files out of it, trying to increase free space to average free space on all slaves. This is % of space to be moved out between full scans. For example, if average free space on all slaves is 1TB, and this parameter will be set to 40, Sector will try to move out 400G of randomly selected files from full slave to other places.
Setting this parameter too high can cause side effects, as endless moving of file replicas betweens slaves at some conditions. 30 can be a reasonable value.

REPLICATE_ON_TRANSACTION_CLOSE
As soon as transaction close, file will be submitted to replication queue. This will allow to very fast replication process, as it does not need to wait for full scan to detect uder/overreplicated file.
Value is TRUE or FALSE.

CHECK_REPLICA_ON_SAME_IP
When you are having several volumes on same node, only way to handle this in sector is to have several slaves on same node, one slave per volume.
It creates possibility to having replicas on same node (same IP address). this is not possible if you have one slave per host.
Setting this parameter to TRUE will enable check if you have replicas on same IP address, and initiate move them out to other slaves.
As this involve additional overhead on full scnas, you should keep this parameter to FALSE if you have single slave per host.

PCT_SLAVES_TO_CONSIDER
When choosing slave to create replica, slaves are selecting randomly. this number allow to remove from consideration most full slaves, changing space distribution.
For example, setting this parameter to 50 will remove from consideration half of slaves (after rest of factors) that have lower free space.

New command in master

Master have 3 new command - debuginfo, ls_n and df.
debuginfo invoked with sysinfo -d, show active transactions,users and replications.
ls_n works a sls but list of replcias is empty, allowing for much smaller message size on large directories, improving responce time for large directories.
It is used by fuse.
df - returns information needed for fuse df command to work, only total size fo file sin sector and free availabel space. It uses only on full traverse of directory and more efficient that use of sysinfo. df is used bu fuse.

Due to new commands, client part of Sector stable branch is not compatible with older versions. Forward compatibility (older client to new master from stable branch) is working. I.e. it is forward compatible but not backward.

Logging
There is significant change of logging. Logging is done on 5 levels -
screen,error,info,warning,trace,debug. Logs format changed, now also showing thread.
Thread showed in square brackets (TID means Thread ID, unfortunatley same as
TID for Transaction ID).
Rather extensive logging for client tools and fuse added. Logs on client side
will be created in /tmp.
Slaves also have rather extensive logs. Location of slave logs - .log in slave
data directory.
Security server throw logs on stdout, usually supressed at start. To see logs
from security server, save stdout.
Master have a lot of information in logs. Location of master logs -
/tmp/sector/master/.log.

Known issues and todos in stable branch
Fuse occasionally failing (very rare, once in month under very specific loads). At same time for majority of application it stays up forever without problems.
It is possible to crash master with massive replication change (decrease of number of replicas).
For example, create 10000 files with replication=3. After replication process will create 3 replicas of each file, change replication to 1. In several minutes master will hang.
Recent version of UDT (UDT 4.10) is not stable with Sector. Sector uses UDT library form April 2011 with 2 bugfixes, not coinciding with any versions of UDT from UDT project on sourceforge.
PCT_SLAVES_TO_CONSIDER does not appear to be working as expected - need to check how it is actually working
Migration to GMP2 is desired but not tested.
Logging level still set in numbers 0-9 in .conf files. Need to migrate to
"screen,error,info,warning,trace,debug). Currenlty numbers mapped to those levels.


