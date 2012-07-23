/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */

#ifndef HASHDB_HH
#define HASHDB_HH 1

#include <kchashdb.h>
#define BUCKET_SIZE 100000000

class HashDB {

    public:
        HashDB(uint64_t csize, std::string fn):
            cache_size(csize), dbfile(fn) {

            db.tune_alignment(0);
            db.tune_buckets(BUCKET_SIZE);
            db.tune_map(cache_size);
            assert(db.open(dbfile.c_str(), kyotocabinet::HashDB::OWRITER | kyotocabinet::HashDB::OCREATE));
        }
        
        /**
        * Insert if already not present
        * return true if insert succeeds
        */
        bool add(std::string key) { 
            std::string val;
            if (!db.get(key, &val))
            {
                if(!db.set(key,"")) {
                    std::cout<<"ERROR: Unable to set into Cache"<<std::endl;
                    exit(1);
                }

                return true;
            } else {
                return false;
            }

        }
        
        uint64_t cache_size;
        std::string dbfile;
        kyotocabinet::HashDB db;

};

#endif
