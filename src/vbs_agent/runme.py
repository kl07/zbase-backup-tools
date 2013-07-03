import vbs_agent

import time
import json
import pdb

vbs_agent.vbs_config_ptr = vbs_agent.create_vbs_config("localhost", 14000)
vbs_agent.start_vbs_config(vbs_agent.vbs_config_ptr)

while 1:
    time.sleep(100)
    config_data = vbs_agent.get_current_config()
    json_string = json.loads(config_data)




