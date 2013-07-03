%module vbs_agent

%{
#include "vbs_agent.h"
%}

/* global variables */

%{
   extern vbs_config_t *vbs_config_ptr;
%}


%inline %{
    extern vbs_config_t *create_vbs_config(char *host, int port);
    extern int start_vbs_config(vbs_config_t *config);
    extern char *get_current_config();
%}
