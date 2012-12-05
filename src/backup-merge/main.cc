/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */

#include <set>
#include <list>
#include <string>
#include <fstream>
#include <iostream>
#include <stdio.h>
#include <assert.h>
#include <unistd.h>
#include <ctype.h>
#include <string.h>
#include <libgen.h>
#include "backup.hh"

int main(int argc, char **argv) {
    int c;
    size_t split_size(0);
    int required = 0;
    bool validation(false);
    std::list <std::string> files;
    std::fstream ifs;
    std::string inputfile, outputfile, workbuffdir;
    std::string buffer;
    char *dirpath;

    while ((c = getopt (argc, argv, "i:o:s:b:v")) != -1) {
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
            case 'b':
                workbuffdir = optarg;
                required++;
                break;
            case 's':
                split_size = atoi(optarg);
                break;
            case 'v':
                validation = true;
                break;
            case '?':
                if (optopt == 'i' || optopt == 'o' || optopt == 'd')
                    std::cout<<"Option -"<<(char) optopt<<" requires argument"<<std::endl;
                exit(1);
            default:
                exit(1);
        }   
    }
    
    if (required != 3) {
        std::cout<<"Usage: "<<argv[0]<<" -i files.txt -o file-%.mbb -b /dev/shm/buffdir/ [-s 512] [-v]"<<std::endl;
        exit(1);
    }

    dirpath = new char[outputfile.length()+1];
    strcpy(dirpath, outputfile.c_str());
    if (access(dirname(dirpath), W_OK)) {
        std::cout<<"Permission denied. (Please enable write permission for target directory)"<<std::endl;
        exit(1);
    }
    delete [] dirpath;

    if (access(inputfile.c_str(), R_OK)) {
        std::cout<<"Unable to access backup file list"<<std::endl;
        exit(1);
    }

    std::cout<<"Merging backup files:"<<std::endl;
    ifs.open(inputfile.c_str(), std::fstream::in);
    while (ifs>>buffer) {   
        if (access(buffer.c_str(), R_OK)) {
            std::cout<<"ERROR: File "<<buffer<<" cannot be accessed"<<std::endl;
            exit(1);
        }

        files.push_back(buffer);
        std::cout<<buffer<<std::endl;
    }   
    std::cout<<std::endl;

    Merge merge(files, outputfile, split_size, validation, workbuffdir);
    merge.process();

    return 0;
}
