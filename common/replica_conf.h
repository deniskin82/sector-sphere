#ifndef REPLICA_CONF_H
#define REPLICA_CONF_H

#include <string>
#include <map>
#include <vector>


struct ReplicaConfData
{
  public:
    ReplicaConfData();
    std::string toString() const;
    int getReplicaNum( const std::string& path, int default_val ) const;
    int getMaxReplicaNum( const std::string& path, int default_val ) const;
    int getReplicaDist( const std::string& path, int default_val ) const;
    void getRestrictedLoc( const std::string& path, std::vector<int>& loc ) const;

  public:
    std::map<std::string, std::pair<int,int> > m_mReplicaNum;                  // number of replicas and max_replicas
    std::map<std::string, int>                 m_mReplicaDist;                 // distance of replicas
    std::map<std::string, std::vector<int> >   m_mRestrictedLoc;               // restricted locations for certain files
    int                                        m_iReplicationStartDelay;       // Delay in sec of replcation thread start on master start
    unsigned                                   m_iReplicationFullScanDelay;    // Min time in sec between full scans by replica thread
    int                                        m_iReplicationMaxTrans;         // Max no of concurrent replications
    int                                        m_iDiskBalanceAggressiveness;   // Percent of full slave files from average free space on all slaves 
                                                                             // to be moved out
    bool                                       m_bReplicateOnTransactionClose; // Submit file into replciation queue on non-read transaction close 
    bool                                       m_bCheckReplicaOnSameIp;        // Check if replica on slaves on same ip
    int                                        m_iPctSlavesToConsider;         // Pct of slaves to consider as replica destination 
    bool				       m_bCheckReplicaCluster;         // check if replica on correct cluster
};


// Note: there is no synchronization provided at all for the replica configuration.
class ReplicaConfig
{
  public:
    static void setPath( const std::string& path );
    static bool readConfigFile();
    static const ReplicaConfData& getCached();
};

#endif

