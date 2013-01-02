/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */

#include "backup.hh"
#include "timing.hh"

Backup::Backup(std::string fn, int m, std::string bfr_path): 
    filename(fn), mode(m), maxsize(0), buffer_path(bfr_path) {
    int flags;
    if (mode & BACKUP_RD_ONLY || mode & BACKUP_CP_RD_ONLY) {
        flags = SQLITE_OPEN_READONLY;
    } else if (mode & BACKUP_WR_ONLY) {
        flags = SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE
            | SQLITE_OPEN_PRIVATECACHE | SQLITE_OPEN_NOMUTEX;
    } else {
        std::cout<<"ERROR: Invalid Backup open flag"<<std::endl;
        exit(1);
    }

    if (mode & BACKUP_TMPFS_BACKEND) {
        genTempFileName();
        if (mode & BACKUP_RD_ONLY) {
            copyfile(filename, tmpfs_file, false, false);
        }

        db = new SQLiteDB(tmpfs_file, flags);
    } else {
        db = new SQLiteDB(filename, flags);
    }

    if (mode & BACKUP_RD_ONLY) {
        stmts = new Statements(db, STMT_READ_OP | STMT_READ_CP);
    } else if(mode & BACKUP_CP_RD_ONLY) {
        stmts = new Statements(db, STMT_READ_CP);
    } else if(mode & BACKUP_WR_ONLY) {
        db->set_journal_mode("OFF");
        db->setVersion(BACKUP_VERSION);

        stmts = new Statements(db, STMT_CREATE_TABLE_OP);
        (stmts->create_table_op())->execute();
        delete stmts;

        stmts = new Statements(db, STMT_CREATE_TABLE_CP);
        (stmts->create_table_cp())->execute();
        delete stmts;

        stmts = new Statements(db, STMT_INSERT_OP | STMT_INSERT_CP
                | STMT_CREATE_INDEX);

        db->start_transaction();
    } else {
        std::cout<<"ERROR: Invalid Statement init flags"<<std::endl;
        exit(1);
    }
}

BACKUP_OPERATION_STATUS Backup::getOperation(Operation **op) {
    bool rv;
    PreparedStatement *st = stmts->read_op();

    rv = st->fetch();
    if (rv) {
        *op = new Operation(
                st->column_int(read_exp_idx),
                st->column(read_key_idx),
                st->column_bytes(read_key_idx),
                st->column(read_op_idx),
                st->column_bytes(read_op_idx),
                st->column_blob(read_val_idx),
                st->column_bytes(read_val_idx),
                st->column_int(read_vbucket_id_idx),
                st->column_int64(read_cpoint_idx),
                st->column_int(read_flag_idx),
                st->column_int64(read_cas_idx),
                st->column_int64(read_seq_idx)
                );
        return OP_RD_SUCCESS;
    } else {
        removefile(tmpfs_file);
        return OP_RD_COMPLETE;
    }
}

BACKUP_OPERATION_STATUS Backup::putOperation(const Operation *op) {
    bool rv;
    PreparedStatement *st = stmts->insert_op();
    if (maxsize && db->getSize() > maxsize) {
        return OP_BACKUP_FULL;
    }

    st->reset();
    st->bind(insert_vbucket_id_idx, op->getVBId());
    st->bind64(insert_cpoint_id_idx, op->getCheckpointId());
    st->bind64(insert_seq_idx, op->getSeq());
    st->bind(insert_op_idx, op->getOp().c_str(), op->getOp().length());
    st->bind(insert_key_idx, op->getKey().c_str(), op->getKey().length());
    st->bind(insert_flag_idx, op->getFlag());
    st->bind(insert_exp_idx, op->getExp());
    st->bind(insert_cas_idx, op->getCas());
    st->bind_blob(insert_blob_idx, op->getBlob(), op->getBlobLen());

    st->execute();

    return OP_WRITE_SUCCESS;
}

void Backup::setSize(size_t sz) {
    maxsize = sz;
}

int Backup::getVersion() {
    return db->getVersion();
}

BACKUP_OPERATION_STATUS Backup::getCheckpoints(std::list<Checkpoint>& checkpoints) {
    PreparedStatement *st = stmts->read_cp();
    Checkpoint *cp;

    while (st->fetch()) {
        cp = new Checkpoint(
                st->column_int(read_cs_vbid_idx),
                st->column_int64(read_cs_cp_idx),
                st->column_int64(read_cs_pcp_idx),
                st->column(read_cs_src_idx),
                st->column_bytes(read_cs_src_idx),
                st->column(read_cs_upd_idx),
                st->column_bytes(read_cs_upd_idx)
                );
        checkpoints.push_back(*cp);
        delete cp;
    }
    return OP_CPOINTS_RD_SUCCESS;
}

BACKUP_OPERATION_STATUS Backup::putCheckpoints(std::list<Checkpoint> checkpoints) {
    PreparedStatement *st = stmts->insert_cp();
    std::list<Checkpoint>::iterator it;
    for (it=checkpoints.begin(); it!=checkpoints.end() ; ++it) {
        st->reset();
        st->bind(insert_cs_vbid_idx, (*it).getVBId());
        st->bind(insert_cs_cp_idx, (*it).getCheckpointId());
        st->bind(insert_cs_pcp_idx, (*it).getPrevCheckpointId());
        st->bind(insert_cs_st_idx, "closed", 6);
        st->bind(insert_cs_src_idx, (*it).getSource().c_str(), (*it).getSource().length());
        st->bind(insert_cs_upd_idx, (*it).getDateTime().c_str(), (*it).getDateTime().length());
        st->execute();
    }
    return OP_CPOINTS_WR_SUCCESS;
}

Backup::~Backup() {
    PreparedStatement *st;

    if (mode & BACKUP_WR_ONLY) {
        st = stmts->create_index();
        st->execute();
        db->commit_transaction();
        delete stmts;
        delete db;

        if (mode & BACKUP_TMPFS_BACKEND) {
            movefile(tmpfs_file, filename, false, false);
        }
    } else {
        delete stmts;
        delete db;
    }
}

void Backup::genTempFileName() {
    std::stringstream ss;

    if (mode & BACKUP_RD_ONLY) {
        ss<<buffer_path<<"input-backup-"<<getpid()<<".mbb";
        tmpfs_file = ss.str();
    } else if (mode & BACKUP_WR_ONLY) {
        ss<<buffer_path<<"output-backup-"<<getpid()<<".mbb";
        tmpfs_file = ss.str();
    }
}

Statements::Statements(SQLiteDB *db, int stmts) {

    ins_op = ins_cp = rd_op = rd_cp = cr_index = cr_table_op = cr_table_cp = NULL;

    if (stmts & STMT_CREATE_TABLE_OP) {
        cr_table_op = new PreparedStatement(db->getDB(), 
                "CREATE TABLE cpoint_op"
                "(vbucket_id integer, cpoint_id integer, seq integer, op text, "
                "key varchar(250), flg integer, exp integer, cas integer, val blob);");
    }

    if (stmts & STMT_CREATE_TABLE_CP) {
        cr_table_cp = new PreparedStatement(db->getDB(), 
                "CREATE TABLE cpoint_state"
                "(vbucket_id integer, cpoint_id integer, prev_cpoint_id integer, state varchar(1), "
                "source varchar(250), updated_at text);");
    }

    if (stmts & STMT_CREATE_INDEX) {
        cr_index = new PreparedStatement(db->getDB(),
                "CREATE INDEX K ON cpoint_op(vbucket_id, key)");
    }

    if (stmts & STMT_INSERT_OP) {
        ins_op = new PreparedStatement(db->getDB(),
                "INSERT INTO cpoint_op"
                "(vbucket_id, cpoint_id, seq, op, key, flg, exp, cas, val)"
                " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)");
    }

    if (stmts & STMT_READ_OP) {
        rd_op = new PreparedStatement(db->getDB(), "SELECT vbucket_id,op,key,flg,exp,cas,val,cpoint_id,seq "
                "FROM cpoint_op");
    }

    if (stmts & STMT_READ_CP) {
        rd_cp = new PreparedStatement(db->getDB(), "SELECT vbucket_id, cpoint_id, prev_cpoint_id, state, source, updated_at "
                "FROM cpoint_state");
    }

    if (stmts & STMT_INSERT_CP) {
        ins_cp = new PreparedStatement(db->getDB(),
                "INSERT into cpoint_state"
                "(vbucket_id, cpoint_id, prev_cpoint_id, state, source, updated_at)"
                " VALUES (?, ?, ?, ?, ?, ?)");
    }

}

Statements::~Statements() {
    delete ins_op;
    delete ins_cp;
    delete rd_op;
    delete rd_cp;
    delete cr_table_op;
    delete cr_table_cp;
    delete cr_index;
}

Operation::Operation(uint32_t exp_val, const char *key_val, size_t key_size_val, const char *op_val,
            size_t op_size_val, const void *blob_val, size_t blob_size_val, uint32_t vbid_val,
            uint64_t cpoint_id_val, uint32_t flags_val, uint64_t cas_val, uint64_t seq_val):
        exp(exp_val), blob_size(blob_size_val),
        vbid(vbid_val), cpoint_id(cpoint_id_val), flags(flags_val), cas(cas_val), seq(seq_val) {

    key.assign(key_val, key_size_val);
    op.assign(op_val, op_size_val);
    blob = new char[blob_size];
    memcpy(blob, (char *) blob_val, blob_size);
}

Operation::Operation(const Operation& operation):
        exp(operation.exp), blob_size(operation.blob_size), vbid(operation.vbid), cpoint_id(operation.cpoint_id),
        flags(operation.flags), cas(operation.cas), seq(operation.seq) {

    key = operation.key;
    op = operation.op;
    blob = new char[operation.blob_size];
    memcpy(blob, operation.blob, blob_size);
}

Operation:: ~Operation() {
    delete [] blob;
}

void CheckpointValidator::addCheckpointList(std::list<Checkpoint>& cplist, std::string filename) {
        std::map<uint64_t, std::list<Checkpoint> > cpMap;
        std::map<uint64_t, std::list<Checkpoint> >::iterator curr_map_it, last_map_it;
        bool repeated_cpoints(false);

        cplist.sort();
        cplist.reverse();
        std::list<Checkpoint>::iterator it = cplist.begin();

        for (; it!=cplist.end(); it++) {
            curr_map_it = cpMap.find((*it).getVBId());
            if (curr_map_it == cpMap.end()) {
                std::list<Checkpoint> lst;
                lst.push_back(*it);
                cpMap[(*it).getVBId()] = lst;

            } else {
                if ((*curr_map_it).second.back().getCheckpointId() != (*it).getCheckpointId()+1) {
                    std::cout<<"ERROR: Checkpoint validation failed within the file, "<<filename<<std::endl;
                    exit(1);
                }
                (*curr_map_it).second.push_back(*it);
            }
        }

        if (first_backup) {
            first_backup = false;
        } else {
            for (curr_map_it=cpMap.begin(); curr_map_it!=cpMap.end(); curr_map_it++) {
                last_map_it = lastFileVBCheckpointMap.find((*curr_map_it).first);
                if ((*last_map_it).second == (*curr_map_it).second) {
                    repeated_cpoints = true;
                } else if ((*last_map_it).second.back().getCheckpointId() == (*curr_map_it).second.front().getCheckpointId() ||
                        (*last_map_it).second.back().getCheckpointId() == (*curr_map_it).second.front().getCheckpointId() + 1) {
                    ;
                } else {
                    if (validation) {
                        std::cout<<"ERROR: VB:"<<(*curr_map_it).first<<" Checkpoint mismatch between files "
                            <<filename<<" ("<<(*curr_map_it).second.front().getCheckpointId()<<") and "
                            <<last_file<<" ("<<(*last_map_it).second.back().getCheckpointId()<<")."<<std::endl;
                        exit(1);
                    }
                }
            }
        }

        lastFileVBCheckpointMap = cpMap;
        if (!repeated_cpoints) {
            checkpointList.insert(checkpointList.end(), cplist.begin(), cplist.end());
        }
        
        last_file = filename;

}

bool Merge::walk_files(std::list <std::string> &files, bool validate) {
    CheckpointValidator cv(validate);
    std::list<Checkpoint> cplist;
    Backup *backup;
    std::list<std::string>::iterator it;

    for (it=files.begin(); it!=files.end(); it++) {
        backup = new Backup(*it, BACKUP_CP_RD_ONLY);

        backup->getCheckpoints(cplist);
        if (backup->getVersion() > BACKUP_VERSION) {
            std::cout<<"ERROR: Backup version should be <= "<<BACKUP_VERSION<<" for "<<*it<<std::endl;
            exit(1);
        }
        cv.addCheckpointList(cplist, *it);
        delete backup;
        cplist.clear();
    }

    cv.getCheckpointList(checkpoints);
}

Merge::Merge(std::list <std::string> files, std::string output_file, int split, bool validate):
    source_files(files), output_file_pattern(output_file), split_size(split), validation(validate) {

        walk_files(source_files, validation);
        keyhash = new HashTable;
}

Merge::~Merge() {
    delete keyhash;
}

void Merge::process() {
    bool ishashinsert;

    Operation *op;
    Backup *ibackup, *obackup;
    BACKUP_OPERATION_STATUS op_rv;
    std::string outfile;
    int split_no(0);

    Timing t_ip_backup_init("Backup - copy to tmpfs"),
           t_op_backup_dest("Backup - copy to disk");

    std::list <std::string>::iterator f;
    f = source_files.begin();
    outfile = genfilename(output_file_pattern, split_no);
    std::cout<<"Creating backup file - "<<outfile<<std::endl;

    obackup = new Backup(outfile, BACKUP_WR_ONLY | BACKUP_TMPFS_BACKEND);
    obackup->setSize(split_size*1024*1024);
    obackup->putCheckpoints(checkpoints);

    while (f != source_files.end()) {
        Timing t_total("Total"), t_hashinsert("HashDB insert"),
               t_dbinsert("Backup insert"), t_dbread("Backup read");

        std::cout<<"Processing file - "<<*f<<std::endl;

        t_ip_backup_init.start();
        ibackup = new Backup(*f, BACKUP_RD_ONLY | BACKUP_TMPFS_BACKEND);
        t_ip_backup_init.stop();
        t_ip_backup_init.display();
        t_ip_backup_init.reset();

        op_rv = ibackup->getOperation(&op);

        while (op_rv != OP_RD_COMPLETE) {
            t_hashinsert.start();
            ishashinsert = keyhash->add(op->getKey());
            t_hashinsert.stop();

            if (ishashinsert) {
                t_dbinsert.start();
                op_rv = obackup->putOperation(op);
                t_dbinsert.stop();

                if (op_rv == OP_BACKUP_FULL) {
                    t_op_backup_dest.start();
                    delete obackup;
                    obackup = NULL;
                    t_op_backup_dest.stop();
                    t_op_backup_dest.display();
                    t_op_backup_dest.reset();

                    split_no++;
                    outfile = genfilename(output_file_pattern, split_no);
                    std::cout<<"Creating backup file - "<<outfile<<std::endl;
                    obackup = new Backup(outfile, BACKUP_WR_ONLY | BACKUP_TMPFS_BACKEND);
                    obackup->setSize(split_size*1024*1024);
                    obackup->putCheckpoints(checkpoints);
                    op_rv = obackup->putOperation(op);

                }
            }

            delete op;
            t_dbread.start();
            op_rv = ibackup->getOperation(&op);
            t_dbread.stop();
        }
        delete ibackup;
        f++;

        
    }
    t_op_backup_dest.start();
    if (obackup != NULL) {
        delete obackup;
        t_op_backup_dest.stop();
        t_op_backup_dest.display();
    }
}

