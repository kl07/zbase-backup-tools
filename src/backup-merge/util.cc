/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */

#include "util.hh"

void copyfile(std::string src, std::string dest, bool nocache_input, bool nocache_output) {
    std::stringstream cmd;

    cmd<<"dd";
    if (nocache_input)
    {
        cmd<<" iflag=direct";
    }

    if (nocache_output) {
        cmd<<" oflag=direct";
    }
    cmd<<" bs=512k if="<<src<<" of="<<dest<<" 2> /dev/null";
    if (system(cmd.str().c_str()) != 0) {
        exit(EXIT_COPYFAIL);
    }
}

void removefile(std::string file) {
    std::stringstream cmd;

    cmd<<"rm -f "<<file;
    if (system(cmd.str().c_str()) == 0) {
        exit(EXIT_REMOVEFAIL);
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
