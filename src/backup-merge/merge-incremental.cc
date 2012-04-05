/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */

#include "merge-incremental.hh"
#include <stdio.h>
#include <ctype.h>
#include <stdlib.h>
#include <unistd.h>
#include <iostream>
#include <list>
#include <fstream>
#include <sqlite3.h>
#include <string>
#include <string.h>
#include <stdint.h>
#include <map>
#include <assert.h>
#include <libgen.h>

using namespace std;

static const int vbucket_id_idx = 0; 
static const int op_idx = 1;
static const int key_idx = 2;
static const int flag_idx = 3;
static const int exp_idx = 4;
static const int cas_idx = 5;
static const int val_idx = 6;
static const int cpoint_idx = 7;
static const int seq_idx = 8;

// Queries
const char *backup_schema = 
        "BEGIN; "
        "CREATE TABLE IF NOT EXISTS cpoint_op" 
        "(vbucket_id integer, cpoint_id integer, seq integer, op text, "
        "key varchar(250), flg integer, exp integer, cas integer, val blob,"
        "primary key(vbucket_id, key));"
        "CREATE TABLE IF NOT EXISTS cpoint_state"
        "(vbucket_id integer, cpoint_id integer, prev_cpoint_id integer, state varchar(1), "
        "source varchar(250), updated_at text);"
        "COMMIT;" ;

const char *insert_query =
        "INSERT OR IGNORE into cpoint_op"
        "(vbucket_id, cpoint_id, seq, op, key, flg, exp, cas, val)"
        " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)" ;

const char *read_query =
        "select cpoint_op.vbucket_id,op,key,flg,exp,cas,val,cpoint_id,seq "
        "from cpoint_op ";

const char *insert_cstate =
        "INSERT OR IGNORE into cpoint_state"
        "(vbucket_id, cpoint_id, prev_cpoint_id, state, source, updated_at)"
        " VALUES (?, ?, ?, ?, ?, ?)";

const char *cpoint_read = "SELECT cpoint_id FROM cpoint_state";

Operation::Operation(uint32_t exp_val, char *key_val, int key_size_val, char *op_val,
            int op_size_val, char *blob_val, int blob_size_val, uint16_t vbid_val, 
            uint64_t cpoint_id_val, uint32_t flags_val, uint64_t cas_val, uint64_t seq_val):
        exp(exp_val), key_size(key_size_val), op_size(op_size_val), blob_size(blob_size_val),
        vbid(vbid_val), cpoint_id(cpoint_id_val), flags(flags_val), cas(cas_val), seq(seq_val) {

    key = new char[key_size];
    op = new char[op_size];
    blob = new char[blob_size];
    memcpy(key, key_val, key_size);
    memcpy(op, op_val, op_size);
    memcpy(blob, blob_val, blob_size);
}

Operation::Operation(const Operation& operation):
        exp(operation.exp), key_size(operation.key_size), op_size(operation.op_size),
        blob_size(operation.blob_size), vbid(operation.vbid), cpoint_id(operation.cpoint_id),
        flags(operation.flags), cas(operation.cas), seq(operation.seq) {

    key = new char[operation.key_size];
    op = new char[operation.op_size];
    blob = new char[operation.blob_size];
    memcpy(key, operation.key, key_size);
    memcpy(op, operation.op, op_size);
    memcpy(blob, operation.blob, blob_size);
}

Operation:: ~Operation() {

    delete [] key;
    delete [] op;
    delete [] blob;
}

bool OutputStore::create_db_name(string &filename) {
    string tmp = output_file;
    size_t tpos = tmp.find("%");
    if (tpos > tmp.length()) {
        filename = tmp;
        return false;
    }
    else {
        char *buffer = new char[tmp.length() + 5];
        tmp.erase(tpos, 1);
        tmp.insert(tpos, "%05d");
        sprintf(buffer, tmp.c_str(), split_number);
        filename = buffer; 
        return true;
    }
}

bool OutputStore::initialize_db(string name) {
    set <int>::iterator it;
    sqlite3_stmt *stmt;

    if (sqlite3_open(name.c_str(), &db) != SQLITE_OK) {
        cout<<"ERROR: Unable to open backup "<<name<<endl;
        exit(1);
    }
    else {
        cout<<"Creating backup file - "<<name<<endl;
    }

    assert(sqlite3_exec(db, backup_schema, 0, 0, 0) == SQLITE_OK);
    assert(sqlite3_exec(db, "pragma page_size=1024", 0, 0, 0) == SQLITE_OK);
    assert(sqlite3_exec(db, "pragma count_changes=OFF", 0, 0, 0) == SQLITE_OK);
    assert(sqlite3_exec(db, "pragma temp_store=MEMORY", 0, 0, 0) == SQLITE_OK);
    assert(sqlite3_exec(db, "pragma cache_size=1048576", 0, 0, 0) == SQLITE_OK);
    assert(sqlite3_exec(db, "pragma synchronous=OFF", 0, 0, 0) == SQLITE_OK);
    assert(sqlite3_exec(db, "pragma journal_mode=MEMORY", 0, 0, 0) == SQLITE_OK);

    for (it=checkpoints.begin(); it!=checkpoints.end(); it++) {
        assert(sqlite3_prepare(db, insert_cstate, -1, &stmt, NULL) == SQLITE_OK);
        sqlite3_bind_int(stmt, 1, 0);
        sqlite3_bind_int(stmt, 2, (*it));
        sqlite3_bind_int(stmt, 3, -1);
        sqlite3_bind_text(stmt, 4, "closed", 6, SQLITE_STATIC);
        sqlite3_bind_text(stmt, 5, "backup", 6, SQLITE_STATIC);
        sqlite3_bind_text(stmt, 6, "MERGE", 5, SQLITE_STATIC);
        assert(sqlite3_step(stmt) == SQLITE_DONE);
        assert(sqlite3_finalize(stmt)==SQLITE_OK);
    }

    return true;
}

bool OutputStore::close_db() {
    if (db != NULL) {
        if (enable_split) {
            assert(sqlite3_exec(db, "COMMIT", 0, 0, 0) == SQLITE_OK);
        }

        assert(sqlite3_close(db) == SQLITE_OK);
        db = NULL;
    }
}

OutputStore::OutputStore(string output_file_pattern, set <int> cpoints, int split_size_val): split_number(0), db(NULL), output_file(output_file_pattern), split_size(split_size_val), checkpoints(cpoints) {
    string filename;
    enable_split = create_db_name(filename);

    if (split_size == -1) {
        enable_split = false;
    }

    initialize_db(filename);
    sqlite3_stmt *stmt;

    if (enable_split) {
        assert(sqlite3_exec(db, "BEGIN", 0, 0, 0) == SQLITE_OK);
    }
    
}

bool OutputStore::insert(const Operation *op) {
    sqlite3_stmt *stmt = NULL;
    string filename;
    int page_count = 0;
    assert(sqlite3_prepare(db, insert_query, -1, &stmt, NULL) == SQLITE_OK);
    sqlite3_bind_int(stmt, 1, op->vbid);
    sqlite3_bind_int64(stmt, 2, op->cpoint_id);
    sqlite3_bind_int64(stmt, 3, op->seq);
    sqlite3_bind_text(stmt, 4, op->op, op->op_size, SQLITE_STATIC);
    sqlite3_bind_text(stmt, 5, op->key, op->key_size, SQLITE_STATIC);
    sqlite3_bind_int(stmt, 6, op->flags);
    sqlite3_bind_int(stmt, 7, op->exp);
    sqlite3_bind_int64(stmt, 8, op->cas);
    sqlite3_bind_blob(stmt, 9, op->blob, op->blob_size, SQLITE_STATIC);
    assert(sqlite3_step(stmt) == SQLITE_DONE);
    assert(sqlite3_finalize(stmt)==SQLITE_OK);

    assert(sqlite3_prepare(db, "pragma page_count;", -1, &stmt, NULL) == SQLITE_OK);
    while (sqlite3_step(stmt) == SQLITE_ROW) {
        page_count = sqlite3_column_int(stmt, 0);
    }

    assert(sqlite3_finalize(stmt)==SQLITE_OK);

    if (enable_split && page_count*1024 > (split_size-1)*1024*1024) {
        split_number += 1;
        close_db();
        create_db_name(filename);
        initialize_db(filename);
        assert(sqlite3_exec(db, "BEGIN", 0, 0, 0) == SQLITE_OK);
    }
}

OutputStore::~OutputStore() {
    close_db();
}

InputStore::InputStore(string file) {

    if (sqlite3_open(file.c_str(), &db) != SQLITE_OK) {
        cout<<"Unable to open database "<<file<<endl;
    }
    assert(sqlite3_exec(db, backup_schema, 0, 0, 0) == SQLITE_OK);
    assert(sqlite3_exec(db, "pragma page_size=1024", 0, 0, 0) == SQLITE_OK);
    assert(sqlite3_exec(db, "pragma count_changes=OFF", 0, 0, 0) == SQLITE_OK);
    assert(sqlite3_exec(db, "pragma temp_store=MEMORY", 0, 0, 0) == SQLITE_OK);
    assert(sqlite3_exec(db, "pragma cache_size=1048576", 0, 0, 0) == SQLITE_OK);
    assert(sqlite3_exec(db, "pragma synchronous=OFF", 0, 0, 0) == SQLITE_OK);
    assert(sqlite3_exec(db, "pragma journal_mode=MEMORY", 0, 0, 0) == SQLITE_OK);
}

bool InputStore::read() {
    sqlite3_stmt *stmt = NULL;
    if (sqlite3_prepare_v2(db, read_query, strlen(read_query), &stmt, NULL) != SQLITE_OK) {
        (void) sqlite3_finalize(stmt);
        (void) sqlite3_close(db);
        cout<<"ERROR: Reading from backup failed (sqlite prepare)"<<endl;
        return false;
    }

    int rc;
    while ((rc = sqlite3_step(stmt)) != SQLITE_DONE) {
        if (rc == SQLITE_ROW) {
            Operation op(
                    sqlite3_column_int(stmt, exp_idx),
                    (char*)sqlite3_column_text(stmt, key_idx),
                    sqlite3_column_bytes(stmt, key_idx),
                    (char *) sqlite3_column_text(stmt, op_idx),
                    sqlite3_column_bytes(stmt, op_idx),
                    (char *) sqlite3_column_text(stmt, val_idx),
                    sqlite3_column_bytes(stmt, val_idx),
                    (uint16_t)sqlite3_column_int(stmt, vbucket_id_idx),
                    (uint64_t)sqlite3_column_int64(stmt, cpoint_idx),
                    sqlite3_column_int(stmt, flag_idx),
                    sqlite3_column_int64(stmt, cas_idx),
                    sqlite3_column_int64(stmt, seq_idx));
            operations.push_back(op);
        }
        else if (rc != SQLITE_BUSY) {
            cout<<"ERROR: Unable to read from backup (sqlite error)"<<endl;
            return false;
        }
    }

    assert(sqlite3_finalize(stmt) == SQLITE_OK);
    assert(sqlite3_close(db) == SQLITE_OK);
    return true;
}

InputStore::~InputStore() {
    operations.clear();
}

Merge::Merge(list <string> files, string output_file, int split, bool validate): 
    source_files(files), output_file_pattern(output_file), split_size(split), validation(validate) {

        bool valid = walk_files(source_files, validation);
        if (validation && !valid) {
            exit(1);
        }

        target_store = new OutputStore(output_file_pattern, checkpoints, split_size);
}

Merge::~Merge() {
    delete target_store;
}

void Merge::process() {
    list <string>::iterator f;
    f = source_files.begin();

    unsigned int s,e;

    while (f != source_files.end()) {
#ifdef SHOW_TIME
        s = time(NULL);
#endif
        cout<<"Processing file - "<<*f<<endl;
        InputStore is(*f);
        is.read();
        list <Operation>::iterator it;
        for (it = is.operations.begin(); it != is.operations.end(); it++) {
            Operation *op = &(*it);
            
            string key(op->key, 0, op->key_size);
            if (klist.count(key) == 0) {
                klist[key] = true;
                target_store->insert(op);
            }

        }

        f++;

#ifdef SHOW_TIME
        e = time(NULL);
        cout<<"time = "<<e-s<<endl;
#endif
    }


}

bool Merge::walk_files(list <string> &files, bool validate) {
    sqlite3 *tmp_db;
    sqlite3_stmt *stmt = NULL;
    uint64_t cpoint_id(0), t(0);
    list <uint64_t> cpoint_list, last_cpoint_list;
    list <string>::iterator it;
    list <uint64_t>::iterator citr;

    for (it=files.begin(); it!=files.end(); it++) {
        assert(sqlite3_open((*it).c_str(), &tmp_db) == SQLITE_OK);
        if (sqlite3_prepare_v2(tmp_db, cpoint_read, strlen(cpoint_read), &stmt, NULL) != SQLITE_OK) {
            cout<<"ERROR: Unable to open file "<<(*it)<<endl;
            return false;
        }

        int rc;
        while ((rc = sqlite3_step(stmt)) != SQLITE_DONE) {
            if (rc == SQLITE_ROW) {
                t = sqlite3_column_int64(stmt, 0);
                cpoint_list.push_back(t);
                checkpoints.insert(t);
            }
            else if (rc != SQLITE_BUSY) {
                cout<<"ERROR: Unable to read backup "<<(*it)<<endl;
                return false;
            }

        }
        
        assert(sqlite3_finalize(stmt)==SQLITE_OK);
        assert(sqlite3_close(tmp_db) == SQLITE_OK);

        if (validate) {
            cpoint_list.sort();
            cpoint_list.reverse();

            t = *(cpoint_list.begin());
            for (citr=cpoint_list.begin(); citr!=cpoint_list.end(); citr++) {
                if (!(t == *(citr) || t == *(citr)+1)) {
                    cout<<"ERROR: Missing checkpoint "<<t-1<<" in "<<(*it)<<endl;
                    return false;
                }
                else {
                    if (t == *(citr)+1) {
                        t--;
                    }
                }
            }

            if (cpoint_id == 0) {
                cpoint_id = *(cpoint_list.begin());
            }
            else {
                if (last_cpoint_list != cpoint_list) {
                    if (!(cpoint_id == *(cpoint_list.begin()) || cpoint_id == *(cpoint_list.begin())+1)) {
                        cout<<"ERROR: Checkpoint mismatch in file "<<(*it)<<" last_file_cpoint_id:"<<cpoint_id<<" current_file_cpoint_id:"<<*(cpoint_list.begin())<<endl;
                        return false;
                    }
                    else {
                        citr = cpoint_list.end();
                        citr--;
                        cpoint_id = *(citr);
                    }
                }
                else {
                    citr = cpoint_list.end();
                    citr--;
                    cpoint_id = *(citr);
                }
            }
        }
        last_cpoint_list = cpoint_list;
        cpoint_list.clear();

    }
    return true;
}

int main(int argc, char **argv) {
    int c;
    int split_size(-1);
    int required = 0;
    bool validation(false);
    list <string> files;
    fstream ifs;
    string inputfile, outputfile;
    string buffer;

    while ((c = getopt (argc, argv, "i:o:s:v")) != -1) { 
        switch (c) 
        {   
            case 'i':
                inputfile = optarg;
                required++;
                break;
            case 'o':
                outputfile = optarg;
                required++;
                break;
            case 's':
                split_size = atoi(optarg);
                break;
            case 'v':
                validation = true;
                break;
            case '?':
                if (optopt == 'i' || optopt == 'o')
                    cout<<"Option -"<<(char) optopt<<" requires argument"<<endl;
                return 1;
            default:
                exit(1);
        }   
    }
    
    if (required != 2) {
        cout<<"Usage: "<<argv[0]<<" -i files.txt -o file-%.mbb [-s 512] [-v]"<<endl;
        exit(1);
    }

    if (access(inputfile.c_str(), R_OK)) {
        cout<<"Unable to access backup file list"<<endl;
        exit(1);
    }

    cout<<"Merging backup files:"<<endl;
    ifs.open(inputfile.c_str(), fstream::in);
    while (ifs>>buffer) {   
        if (access(buffer.c_str(), R_OK)) {
            cout<<"ERROR: File "<<buffer<<" cannot be accessed"<<endl;
            exit(1);
        }

        files.push_back(buffer);
        cout<<buffer<<endl;
    }   
    cout<<endl;

    Merge merge(files, outputfile, split_size, validation);
    merge.process();

    return 0;
}
