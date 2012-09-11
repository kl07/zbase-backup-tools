/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */

#ifndef BACKUP_HH
#define BACKUP_HH

#include "sqlite-objects.hh"
#include "util.hh"
#include "hashtable.hh"
#include <iostream>
#include <list>
#include <map>

#define BACKUP_VERSION 2

class Operation;
class Statements;
class Checkpoint;

enum {
    BACKUP_RD_ONLY=1,
    BACKUP_WR_ONLY=2,
    BACKUP_TMPFS_BACKEND=4,
    BACKUP_CP_RD_ONLY=8
};

typedef enum {
    OP_RD_SUCCESS,
    OP_RD_COMPLETE,
    OP_WRITE_SUCCESS,
    OP_BACKUP_FULL,
    OP_CPOINTS_RD_SUCCESS,
    OP_CPOINTS_WR_SUCCESS,
    OP_INVALID
} BACKUP_OPERATION_STATUS;

enum {
    STMT_INSERT_OP=1,
    STMT_INSERT_CP=2,
    STMT_READ_OP=4,
    STMT_READ_CP=8,
    STMT_CREATE_INDEX=16,
    STMT_CREATE_TABLE_OP=32,
    STMT_CREATE_TABLE_CP=64,
    STMT_CKSUM=128
};


//Constant definitions
enum {
    read_vbucket_id_idx,
    read_op_idx,
    read_key_idx,
    read_flag_idx,
    read_exp_idx,
    read_cas_idx,
    read_val_idx,
    read_cpoint_idx,
    read_seq_idx,
    read_cksum_idx
};

enum {
    insert_vbucket_id_idx = 1,
    insert_cpoint_id_idx,
    insert_seq_idx,
    insert_op_idx,
    insert_key_idx,
    insert_flag_idx,
    insert_exp_idx,
    insert_cas_idx,
    insert_cksum_idx,
    insert_blob_idx
};

enum {
    read_cs_vbid_idx,
    read_cs_cp_idx,
    read_cs_pcp_idx,
    read_cs_src_idx,
    read_cs_upd_idx,
};

enum {
    insert_cs_vbid_idx = 1,
    insert_cs_cp_idx,
    insert_cs_pcp_idx,
    insert_cs_st_idx,
    insert_cs_src_idx,
    insert_cs_upd_idx
};

#define TMPFS_PATH "/dev/shm/"

/*
 * Backup storage interface
 */

class Backup {

public:
    Backup(std::string fn, int m, std::string bfr_path=TMPFS_PATH);

    BACKUP_OPERATION_STATUS getOperation(Operation **op);

    BACKUP_OPERATION_STATUS putOperation(const Operation *op);

    void setSize(size_t sz);

    int getVersion();

    BACKUP_OPERATION_STATUS getCheckpoints(std::list<Checkpoint>& checkpoints);

    BACKUP_OPERATION_STATUS putCheckpoints(std::list<Checkpoint> checkpoints);

    ~Backup();

    void genTempFileName();

    SQLiteDB *db;
    Statements *stmts;
    std::string buffer_path;
    std::string filename;
    std::string tmpfs_file;
    size_t maxsize;
    int mode;
};

class Statements {
public:
    Statements(SQLiteDB *db, int stmts);

    ~Statements();

    PreparedStatement *insert_op() {
        return ins_op;
    }

    PreparedStatement *insert_cp() {
        return ins_cp;
    }

    PreparedStatement *read_op() {
        return rd_op;
    }

    PreparedStatement *read_cp() {
        return rd_cp;
    }

    PreparedStatement *create_index() {
        return cr_index;
    }

    PreparedStatement *create_table_op() {
        return cr_table_op;
    }

    PreparedStatement *create_table_cp() {
        return cr_table_cp;
    }


private:
    PreparedStatement *ins_op;
    PreparedStatement *ins_cp;
    PreparedStatement *rd_op;
    PreparedStatement *rd_cp;
    PreparedStatement *cr_index;
    PreparedStatement *cr_table_op;
    PreparedStatement *cr_table_cp;
};

/**
 * Operation to store a mutation
 */
class Operation {
public:

    /**
     * Constructor that takes the value and create a mutation object
     */
    Operation(uint32_t exp_val, const char *key_val, size_t key_size_val, const char *op_val,
            size_t op_size_val, const void *blob_val, size_t blob_size_val, uint32_t vbid_val,
            uint64_t cpoint_id_val, uint32_t flags_val, uint64_t cas_val, uint64_t seq_val,
            const char *ck, size_t ck_len);
    /**
     * Copy constructor
     */
    Operation(const Operation& op);

    std::string getKey() const {
        return key;
    }

    std::string getOp() const {
        return op;
    }

    uint32_t getExp() const {
        return exp;
    }

    uint32_t getVBId() const {
        return vbid;
    }

    uint64_t getCheckpointId() const {
        return cpoint_id;
    }

    uint32_t getFlag() const {
        return flags;
    }

    uint64_t getCas() const {
        return cas;
    }

    uint64_t getSeq() const {
        return seq;
    }

    std::string getCksum() const {
        return cksum;
    }

    char *getBlob() const {
        return blob;
    }

    size_t getBlobLen() const {
        return blob_size;
    }


    /**
     * Destructor
     */
    ~Operation();

private:
    uint32_t exp;
    std::string key;
    std::string op;
    char *blob;
    uint32_t vbid;
    uint64_t cpoint_id;
    uint32_t flags;
    uint64_t cas;
    uint64_t seq;
    size_t blob_size;
    std::string cksum;
};

/**
 * Checkpoint metadata storage
 */
class Checkpoint {

public:
     Checkpoint(uint64_t vbid, uint64_t cp_id, uint64_t pcp_id, const char *upd, size_t upd_len, const char *src, size_t src_len): 
            vbucket_id(vbid), checkpoint_id(cp_id), prev_checkpoint_id(pcp_id) {
        datetime.assign(upd, upd_len);
        source.assign(src, src_len);
    }

     Checkpoint(const Checkpoint& cp) {
         vbucket_id = cp.vbucket_id;
         checkpoint_id = cp.checkpoint_id;
         prev_checkpoint_id = cp.prev_checkpoint_id;
         datetime = cp.datetime;
         source = cp.source;
     }

     bool operator== (const Checkpoint& cp) const{
         return checkpoint_id == cp.checkpoint_id;
     }

     bool operator< (const Checkpoint& cp) const{
         return checkpoint_id < cp.checkpoint_id;
     }

    uint64_t getCheckpointId() {
        return checkpoint_id;
    }

    uint64_t getVBId() {
        return vbucket_id;
    }

    uint64_t getPrevCheckpointId() {
        return prev_checkpoint_id;
    }

    std::string getDateTime() {
        return datetime;
    }

    std::string getSource() {
        return source;
    }

private:
    uint64_t vbucket_id;
    uint64_t checkpoint_id;
    uint64_t prev_checkpoint_id;
    std::string datetime;
    std::string source;
};



class CheckpointValidator {

public:
    CheckpointValidator(bool v=true): first_backup(true), validation(v), cplist_dirty(false) {}

    void addCheckpointList(std::list<Checkpoint>& cplist, std::string filename);

    void getCheckpointList(std::list<Checkpoint>& cplist) {
        if (cplist_dirty) {
            checkpointList.sort();
            checkpointList.reverse();
            checkpointList.unique();
            cplist_dirty = false;
        }
        cplist = checkpointList;
    }
private:

    std::list<Checkpoint> checkpointList;
    std::map<uint64_t, std::list<Checkpoint> > lastFileVBCheckpointMap;
    std::string last_file;
    bool validation;
    bool first_backup;
    bool cplist_dirty;

};



/**
 * Merge class which reads the source backup files and merge into deduped
 * sqlite3 files
 */
class Merge {
    bool validation;
    int split_size;
    std::string output_file_pattern;
    HashTable *keyhash;
    std::list <std::string> source_files;
    std::list <Checkpoint> checkpoints;

public:
    /**
     * Initialize
     */
    Merge(std::list<std::string> files, std::string output_file, int split, bool validate);


    /**
     * Walk through the source db files and set populate checkpoints set
     * If validate=true, validate checkpoint ordering in the files
     */
    bool walk_files(std::list <std::string> &files, bool validate);

    /**
     * Start merge processing of files and create output merged split files
     */
    void process();

    /**
     * Destructor
     */
    ~Merge();

};

#endif
