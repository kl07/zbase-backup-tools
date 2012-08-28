/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#ifndef UTIL_HH
#define UTIL_HH 1

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <iostream>
#include <sstream>

#define EXIT_COPYFAIL 2
#define EXIT_REMOVEFAIL 3


void copyfile(std::string src, std::string dest, bool nocache_input=true, bool nocache_output=true);

void removefile(std::string file);

void movefile(std::string src, std::string dest, bool nocache_input=true, bool nocache_output=true);

std::string genfilename(std::string &filename_pattern, int split_no);

#endif
