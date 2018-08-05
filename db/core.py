from utils.remote import Request
from utils.decorators import log_remote_errors

RETVAR = 'retcode'


def make_hcl_cmd(sql):
    return """su - postgres -c "psql -U postgres powerdns -t -c \\"{}\\"" """.format(sql)


@log_remote_errors
def exec_remote_query(host_id, sql):
    request = Request(host_id, 'root', 'root')
    request.command(make_hcl_cmd(sql),
                    valid_exit_codes=[0],
                    stdout='stdout',
                    stderr='stderr',
                    retvar=RETVAR)
    result = request.perform()
    if '0' != result[RETVAR]:
        raise Exception("Remote command fail: {}".format(result))
    return result['stdout'].strip()


def exec_hub_query(con, sql):
    cur = con.cursor()
    cur.execute(sql)
    record_set = cur.fetchall()
    return record_set
