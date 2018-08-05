from utils.decorators import log_start_end

from db.core import exec_remote_query
from db.references import PowerDnsSqlReference


@log_start_end
def update_ttl(host_id, upd_map):
    exec_remote_query(host_id, PowerDnsSqlReference.update_dns_records(upd_map))


@log_start_end
def delete_duplicates(host_id, del_set):
    exec_remote_query(host_id, PowerDnsSqlReference.delete_dns_records(del_set))
