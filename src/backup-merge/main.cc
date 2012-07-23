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
    size_t cache_size(1024); 
    int required = 0;
    bool validation(false);
    std::list <string> files;
    std::fstream ifs;
    std::string inputfile, outputfile, dbfile;
    std::string buffer;
    char *dirpath;

    while ((c = getopt (argc, argv, "i:o:s:c:vd:")) != -1) { 
        switch (c) 
        {   
            case 'i':
                inputfile = optarg;
                required++;
                break;
            case 'd':
                dbfile = optarg;
                required++;
                break;
            case 'o':
                outputfile = optarg;
                required++;
                break;
            case 's':
                split_size = atoi(optarg);
                break;
            case 'c':
                cache_size = atoi(optarg);
                break;
            case 'v':
                validation = true;
                break;
            case '?':
                if (optopt == 'i' || optopt == 'o' || optopt == 'd')
                    cout<<"Option -"<<(char) optopt<<" requires argument"<<endl;
                exit(1);
            default:
                exit(1);
        }   
    }
    
    if (required != 3) {
        cout<<"Usage: "<<argv[0]<<" -i files.txt -o file-%.mbb [-s 512] [-v] -d hashdb_file"<<endl;
        exit(1);
    }

    dirpath = new char[outputfile.length()+1];
    strcpy(dirpath, outputfile.c_str());
    if (access(dirname(dirpath), W_OK)) {
        cout<<"Permission denied. (Please enable write permission for target directory)"<<endl;
        exit(1);
    }
    delete [] dirpath;

    dirpath = new char[dbfile.length()+1];
    strcpy(dirpath, dbfile.c_str());
    if (access(dirname(dirpath), W_OK)) {
        cout<<"Permission denied. (Please enable write permission for hashdb file directory)"<<endl;
        exit(1);
    }
    delete [] dirpath;

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

    Merge merge(files, outputfile, split_size, validation, cache_size, dbfile);
    merge.process();

    return 0;
}
