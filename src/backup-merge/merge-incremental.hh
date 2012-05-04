/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */

#ifndef MERGE_HH
#define MERGE_HH 1

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <ctype.h>
#include <string.h>
#include <sqlite3.h>
#include <stdint.h>
#include <set>
#include <list>
#include <string>
#include <fstream>
#include <iostream>
#include <tr1/unordered_map>

using namespace std;

/**
 * Operation class is used to store a mutation
 */
class Operation {

    uint32_t exp;
    char *key;
    char *op;
    char *blob;
    uint16_t vbid;
    uint64_t cpoint_id;
    uint32_t flags;
    uint64_t cas;
    uint64_t seq;
    size_t blob_size;
    size_t key_size;
    size_t op_size;

public:

    /**
     * Constructor that takes the value and create a mutation object
     */
    Operation(uint32_t exp_val, char *key_val, int key_size_val, char *op_val,
            int op_size_val, char *blob_val, int blob_size_val, uint16_t vbid_val, 
            uint64_t cpoint_id_val, uint32_t flags_val, uint64_t cas_val, uint64_t seq_val);
    /**
     * Copy constructor
     */
    Operation(const Operation& op);

    /**
     * Destructor
     */
    ~Operation();

    friend class OutputStore;
    friend class Merge;
};


/**
 * Storage abstraction for writing mutations to output split db
 */
class OutputStore {
    std::string output_file;
    int split_size;
    int split_number;
    bool enable_split ;
    set <int> checkpoints;
    sqlite3 *db;

    /**
     * Generate next available split database name
     * Eg. backup-%.mbb -> backup-00000.mbb
     */
    bool create_db_name(std::string &filename);

    /**
     * Create sqlite db with given path
     */
    bool initialize_db(std::string name);

    bool close_db();

public:
    /**
     * Initialize object
     */
    OutputStore(std::string output_file_pattern, set <int> cpoints, int split_size_val);

    /**
     * Method to insert a mutation into output store
     */
    bool insert(const Operation *operation);

    /**
     * Destructor
     */
    ~OutputStore();

};

/**
 * Class that models input sqlite files
 */
class InputStore {
    sqlite3 *db;
    list<Operation> operations;

public:
    /**
     * Initialize input db
     */
    InputStore(std::string file);

    /**
     * Read entire data from db into operations list
     */
    bool read();

    /**
     * Iterator begin for operations list
     */
    list<Operation>::iterator begin() {
        return operations.begin();
    }

    /**
     * Iterator end for operations list
     */
    list<Operation>::iterator end() {
        return operations.end();
    }

    /**
     * Destructor
     */
    ~InputStore();

    friend class Merge;
};

/**
 * Merge class which reads the source backup files and merge into deduped
 * sqlite3 files
 */
class Merge {
    OutputStore *target_store;
    bool validation;
    int split_size;
    string output_file_pattern;
    std::tr1::unordered_map <std::string, bool> klist;
    list <std::string> source_files;
    set <int> checkpoints;

    /**
     * Walk through the source db files and set populate checkpoints set
     * If validate=true, validate checkpoint ordering in the files
     */
    bool walk_files(list <string> &files, bool validate);

public:
    /**
     * Initialize
     */
    Merge(list <string> files, string output_file, int split, bool validate);

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
