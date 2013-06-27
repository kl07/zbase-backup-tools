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
