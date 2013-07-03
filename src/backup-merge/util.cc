/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */

/*
 *   Copyright 2013 Zynga Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

#include "util.hh"

void copyfile(std::string src, std::string dest, bool nocache_input, bool nocache_output) {
    std::stringstream cmd;

    int retry(0);

    cmd<<"dd";
    if (nocache_input)
    {
        cmd<<" iflag=direct";
    }

    if (nocache_output) {
        cmd<<" oflag=direct";
    }
    cmd<<" bs=512k if="<<src<<" of="<<dest<<" 2> /dev/null";
    while (system(cmd.str().c_str()) != 0) {
        retry++;
        if (retry == 4) {
            std::cout<<"ERROR: File copy failed ("<<cmd.str()<<")"<<std::endl;
            exit(EXIT_COPYFAIL);
        }
    }
}

void removefile(std::string file) {
    std::stringstream cmd;

    int retry(0);
    cmd<<"rm -f "<<file;

    while (system(cmd.str().c_str()) != 0) {
        retry++;
        if (retry == 4) {
            std::cout<<"ERROR: File remove failed ("<<cmd.str()<<")"<<std::endl;
            exit(EXIT_REMOVEFAIL);
        }
    }
}

void movefile(std::string src, std::string dest, bool nocache_input, bool nocache_output) {
    std::stringstream cmd;
    copyfile(src, dest, nocache_input, nocache_output);
    removefile(src);
}

std::string genfilename(std::string &filename_pattern, int split_no) {
    size_t tpos;
    char *buffer;
    std::string tmp(filename_pattern);

    tpos = tmp.find("%");
    if (tpos > tmp.length()) {
        return tmp;
    } else {
        buffer = new char[tmp.length() + 5];
        tmp.erase(tpos, 1);
        tmp.insert(tpos, "%05d");
        sprintf(buffer, tmp.c_str(), split_no);
        tmp.assign(buffer);
        delete [] buffer;
        return tmp;
    }
}
