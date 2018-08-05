import logging

from db.core import exec_hub_query, exec_remote_query
from db.references import HubSqlReference, PowerDnsSqlReference

from utils.decorators import log_start_end
from utils.utils import flatten_list

logger = logging.getLogger(__name__)


def to_record_set(input_str):
    row_strings = map(lambda row: row.split(' | '), input_str.splitlines())
    record_set = [map(lambda cell: cell.strip(), row) for row in row_strings]
    return record_set


def fetch_powerdns_domains(host_id, sync_domains=None, exclude_domains=None):
    return to_record_set(
        exec_remote_query(
            host_id,
            PowerDnsSqlReference.select_domains(
                sync_domains,
                exclude_domains)))


@log_start_end
def fetch_powerdns_records_by_domain_list(host_id, domain_ids_list):
    sql_select = PowerDnsSqlReference.select_dns_records(domain_ids_list)
    powerdns_records = to_record_set(exec_remote_query(host_id, sql_select))
    return powerdns_records


def fetch_hub_records_by_domain_list(db_conn, domain_names):
    logger.debug("Looking up ID in MN for domains: {}".format(','.join(domain_names)))

    domain_record_set = exec_hub_query(db_conn, HubSqlReference.select_domain_id(domain_names))
    real_size = len(domain_record_set)
    requested_size = len(domain_names)

    if real_size == 0:
        msg = "None of {} domains were found in Hub database!".format(requested_size)
        msg += " Synchronization of DNS records of these domains is skipped."
        logger.warning(msg)
        return (False, [])

    if real_size < requested_size:
        msg = "Only {} of {} domains were found in Hub database!".format(
              real_size, requested_size)
        msg += " Synchronization for {} domain(s) is skipped.".format(requested_size - real_size)
        logger.warning(msg)

    domain_ids_list = flatten_list(domain_record_set)
    return (True, exec_hub_query(db_conn, HubSqlReference.select_dns_records(domain_ids_list)))


# @measure_worktime
@log_start_end
def get_powerdns_hosts(db_conn):
    return flatten_list(exec_hub_query(db_conn, HubSqlReference.select_powerdns_hosts()))


def get_powerdns_report_dict(powerdns_records):
    return {
        rec_id: (idn_host, rr_type, rec_data, ttl, prio, domain_id, domain_name)
        for (rec_id, idn_host, rr_type, rec_data, ttl, prio, domain_id, domain_name, _, _)
        in powerdns_records
    }
