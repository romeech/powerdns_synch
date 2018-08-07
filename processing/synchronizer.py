import logging

from itertools import groupby

from db.mutators import delete_duplicates, update_ttl
from db.selectors import (
    fetch_hub_records_by_domain_list,
    fetch_powerdns_domains,
    fetch_powerdns_records_by_domain_list,
    get_powerdns_report_dict,
)

from utils.utils import flatten_list, split_to_chunks
from utils.decorators import log_start_end


DOMAINS_CHUNK_SIZE = 2000
MAX_UPD_SIZE = 650
MAX_DEL_SIZE = 3500

logger = logging.getLogger(__name__)


def group_powerdns_recs_by_record_hash(powerdns_records):
    """
    [(rec_id, ..., rec_hash,...), ...] -> {rec_hash: [(rec_id,...),], ...}
    """
    sorted_by_rec_hash = sorted(powerdns_records, key=lambda row: row[8])
    return {
        rec_hash: list(recs)
        for rec_hash, recs in groupby(sorted_by_rec_hash, key=lambda row: row[8])
    }


# @measure_worktime
@log_start_end
def match_dns_records(powerdns_records, hub_dns_records):
    # hub_dns_records: [
    #   (rr_type, rec_data, ttl, rec_hash, ttl_hash),
    # ]
    hub_ttl_hashes = {ttl_hash for (_, _, _, _, ttl_hash) in hub_dns_records}
    hub_rechash2ttl = {rec_hash: ttl for (_, _, ttl, rec_hash, _) in hub_dns_records}

    # powerdns_records: [
    #   (rec_id, idn_host, rr_type, rec_data, ttl, prio, domain_id,
    #    domain_name, rec_hash, ttl_hash),
    # ]
    pdns_rec_hash_grps = group_powerdns_recs_by_record_hash(powerdns_records)
    unique_pdns_recs = {rec_hash: recs[0] for rec_hash, recs in pdns_rec_hash_grps.items()}
    duplicate_pdns_recs = flatten_list([recs[1:] for _, recs in pdns_rec_hash_grps.items()])

    duplicate_ids = {row[0] for row in duplicate_pdns_recs}

    pdns_rec_hashes = set(unique_pdns_recs.keys())
    phantom_recs = pdns_rec_hashes.difference(hub_rechash2ttl.keys())
    matching_recs = pdns_rec_hashes.intersection(hub_rechash2ttl.keys())

    del_set = {
        rec_id
        for rec_hash, (rec_id, _, _, _, _, _, _, _, _, _) in unique_pdns_recs.items()
        if rec_hash in phantom_recs
    }.union(duplicate_ids)

    upd_map = {
        rec_id: hub_rechash2ttl[rec_hash]  # relevant TTL value
        for rec_hash, (rec_id, _, _, _, _, _, _, _, _, ttl_hash) in unique_pdns_recs.items()
        if (ttl_hash not in hub_ttl_hashes) and (rec_hash in matching_recs)
    }

    return (upd_map, del_set)


def fix_records(host_id, upd_map, del_set):
    if len(upd_map) or len(del_set):
        logger.info("Fixing a difference on PowerDNS host #{}".format(host_id))

    if len(upd_map):
        upd_chunks = split_to_chunks(upd_map.items(), MAX_UPD_SIZE, lambda x: dict(x))
        for chunck in upd_chunks:
            update_ttl(host_id, chunck)

    if len(del_set):
        del_chunks = split_to_chunks(list(del_set), MAX_DEL_SIZE)
        for chunck in del_chunks:
            delete_duplicates(host_id, chunck)


# @measure_worktime
def report_errors(csv_reporter, host_id, upd_map, del_set, powerdns_rec_dict):
    for (rec_id, new_ttl) in upd_map.items():
        (idn_host,
         rr_type,
         rec_data,
         old_ttl,
         prio,
         domain_id,
         domain_name) = powerdns_rec_dict[rec_id]

        csv_reporter.post_update_required(
            host_id=host_id,
            domain_id=domain_id,
            domain_name=domain_name,
            rec_id=rec_id,
            rr_type=rr_type,
            content=rec_data,
            prio=prio,
            old_ttl=old_ttl,
            new_ttl=new_ttl
        )

    for del_id in del_set:
        idn_host, rr_type, rec_data, ttl, prio, domain_id, domain_name = powerdns_rec_dict[del_id]

        csv_reporter.post_remove_duplicate(
            host_id=host_id,
            domain_id=domain_id,
            domain_name=domain_name,
            rec_id=del_id,
            rr_type=rr_type,
            content=rec_data,
            prio=prio,
            ttl=ttl
        )


def synchronize(
    db_conn, csv_reporter,
    hosts, sync_domains, exclude_domains,
    skip_error_report, fix_errors,
):
    csv_reporter.post_header()

    for host_id in hosts:
        pdns_domains = fetch_powerdns_domains(host_id, sync_domains, exclude_domains)
        if not pdns_domains:
            continue

        processed_count = 0
        domains_count = len(pdns_domains)
        for domain_chunk in split_to_chunks(pdns_domains, DOMAINS_CHUNK_SIZE):
            domain_ids, domain_names = zip(*domain_chunk)
            powerdns_records = fetch_powerdns_records_by_domain_list(host_id, domain_ids)
            hub_requested, hub_dns_records = fetch_hub_records_by_domain_list(db_conn, domain_names)

            logger.info("Processed domains: {}; total: {}".format(processed_count, domains_count))
            logger.info(
                "Synchronizing DNS records of next {} domains between Server and PowerDNS host #{}",
                len(domain_names), host_id
            )

            if hub_requested:
                upd_map, del_set = match_dns_records(powerdns_records, hub_dns_records)

                if not skip_error_report:
                    report_errors(
                        csv_reporter, host_id, upd_map, del_set,
                        get_powerdns_report_dict(powerdns_records)
                    )

                if fix_errors:
                    fix_records(host_id, upd_map, del_set)

            processed_count += len(domain_chunk)

    if not skip_error_report:
        logger.info(
            "Synchronization is complete, a difference report has been created: {}"
            .format(csv_reporter.get_report_path()))
