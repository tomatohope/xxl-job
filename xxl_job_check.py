import os, MySQLdb, sys

# query ways:
''' 
    first : a txt parameter from jenkins is saved as a temporary file
    then  : this script will read these info
    last  : display the different 
    [taskId],[cron],[cmd],
'''

# exec_sql
def execsql(host, user, passwd, database, sql, result):
    db = MySQLdb.connect(host=host, port=3306, user=user, passwd=passwd, db=database)
    cursor = db.cursor()
    cursor.execute(sql)
    results = []
    if str(result) == '1':
        results = cursor.fetchall()
    cursor.close()
    return results

# get_xxl_cron_info
def diff_cron(id, cron, croncmd, host, user, passwd, database, env):
    # get xxl_job_config
    sql = "SELECT id, job_group, job_desc, glue_remark, job_cron, author, alarm_email, executor_route_strategy, executor_block_strategy, glue_source, trigger_status FROM xxl_job_info WHERE ID = " + str(id)
    print("sql:", sql)
    result = execsql(host, user, passwd, database, sql, "1")
    if len(result) == 0:
        print("##############################################")
        print("\n" + "    " + "taskID: " + id + " select result is null" + "\n")
        print("##############################################")
        sys.exit(0)

    print("taskID: ", id, "select_result  :", result)
    print("taskID: ", id, "xxl_config_cron:", result[0][4][2:-2] + " *")
    xxl_config_cron = result[0][4][2:-2] + " *"
    print("taskID: ", id, "xxl_config_cmd :", result[0][9][12:-1])
    xxl_config_cmd = result[0][9][12:-1]
    print("taskID: ", id, "xxl_job_author :", result[0][5])
    print("taskID: ", id, "xxl_job_alarm  :", result[0][6])
    print("taskID: ", id, "xxl_job_desc   :", result[0][2])
    print("taskID: ", id, "xxl_job_remark :", result[0][3])
    print("taskID: ", id, "xxl_job_route  :", result[0][7])
    print("taskID: ", id, "xxl_job_block  :", result[0][8])

    # diff cron
    if xxl_config_cron != cron:
        print("##############################################")
        print("\n" + "    " + "taskID: " + id + " the cron is different" + "\n")
        print("##############################################")
        cron_list_file = xxl_dir + "/cron_list_cron"
        with open(cron_list_file, 'a+') as list_cron:
            list_cron.write("taskID: " + id + " the cron is different" + "\n")
    # diff cmd
    if xxl_config_cmd != croncmd:
        print("##############################################")
        print("\n" + "    " + "taskID: " + id + " the command is different" + "\n")
        print("##############################################")
        cron_list_file = xxl_dir + "/cron_list_cmd"
        with open(cron_list_file, 'a+') as list_cmd:
            list_cmd.write("taskID: " + id + " the cron command is different" + "\n")
    # check route strategy
    if result[0][7] != "BUSYOVER":
        print("##############################################")
        print("\n" + "    " + "taskID: " + id + " the route strategy should be BUSYOVER" + "\n")
        print("##############################################")
        cron_list_file = xxl_dir + "/cron_list_route_strategy"
        with open(cron_list_file, 'a+') as list_route:
            list_route.write("taskID: " + id + " the route strategy should be BUSYOVER" + "\n")
    # check block strategy
    if result[0][8] != "SERIAL_EXECUTION":
        print("##############################################")
        print("\n" + "    " + "taskID: " + id + " the block strategy should be SERIAL_EXECUTION" + "\n")
        print("##############################################")
        cron_list_file = xxl_dir + "/cron_list_block_strategy"
        with open(cron_list_file, 'a+') as list_block:
            list_block.write("taskID: " + id + " the block strategy should be SERIAL_EXECUTION" + "\n")
    # check describe info
    if result[0][2] != result[0][3] or result[0][2].lower().startswith(env + "_"):
        print("##############################################")
        print("\n" + "    " + "taskID: " + id + " the describe info is different " + "eg: " + env + "_" + "projectname_funcion" + "\n")
        print("##############################################")
        cron_list_file = xxl_dir + "/cron_list_desc"
        with open(cron_list_file, 'a+') as list_desc:
            list_desc.write("taskID: " + id + " the describe info is different " + "eg: " + env + "_" + "projectname_funcion" + "\n")
    # judge the same desc
    sql = "SELECT count(*) from xxl_job_info WHERE job_desc = '" + result[0][2] + "'"
    result = execsql(host, user, passwd, database, sql, "1")
    if result[0][0] != 1:
        print("##############################################")
        print("\n" + "    " + "taskID: " + id + " the describe info is already exist" + "\n")
        print("##############################################")
        cron_list_file = xxl_dir + "/cron_list_desc"
        with open(cron_list_file, 'a+') as list_same:
            list_same.write("taskID: " + id + " the describe info is already exist" + "\n")

# input cron from jenkins txt parameter
env = "Dev"
user = "xxx"
database = "xxx"
passwd = "xxx"
host = "xxx"
xxl_dir = "/tmp/xxl_job_review"
os.environ['xxl_dir'] = xxl_dir
cron_lines = int(os.popen("cat $xxl_dir/xxl_job_review | awk -F\",\" '{print NF}'").read())

os.system("cat $xxl_dir/xxl_job_review |awk  -F ',' '{for(i=3;i<=NF;i=i+3) print $(i-2)\",\"$(i-1)\",\"$i}' | sed -e 's/^ //g' > $xxl_dir/cron_lists")
cron_lists_file = xxl_dir + "/cron_lists"

diff_file = xxl_dir + "/cron_list_*"
os.environ['diff_file'] = xxl_dir + "/cron_list_*"
os.system("rm $diff_file -f")
with open(cron_lists_file, 'r') as lists:
    for cronline in lists:
        cronline = cronline.split(",", 2)
        print("cronline         :", cronline)
        print("cronline_taskID  :", cronline[0])
        print("cronline_taskCron:", cronline[1])
        print("cronline_cmd     :", cronline[2].replace("\n", ""))

        diff_cron(cronline[0], cronline[1], cronline[2].replace("\n", ""), host, user, passwd, database, env)

print("##############################################")
if os.path.isfile(xxl_dir + "/cron_list_cron") is True:
    os.system("cat $xxl_dir/cron_list_cron")
else:
    print(" the cron is ok")
if os.path.isfile(xxl_dir + "/cron_list_cmd") is True:
    os.system("cat $xxl_dir/cron_list_cmd")
else:
    print(" the cron cmd is ok")
if os.path.isfile(xxl_dir + "/cron_list_route_strategy") is True:
    os.system("cat $xxl_dir/cron_list_route_strategy")
else:
    print(" the cron route strategy is ok")
if os.path.isfile(xxl_dir + "/cron_list_block_strategy") is True:
    os.system("cat $xxl_dir/cron_list_block_strategy")
else:
    print(" the cron block strategy is ok")
if os.path.isfile(xxl_dir + "/cron_list_desc") is True:
    os.system("cat $xxl_dir/cron_list_desc")
else:
    print(" the cron desc is ok")