import getopt
import os
import sys
import logging

from db import db_client
from db.selectors import get_powerdns_hosts

from processing.synchronizer import synchronize
from processing.reporters import PowerDnsSyncCsvReporter

from utils.utils import timestamp


logger = logging.getLogger(__name__)

__long_options = [
    ('fix-errors',
        "The script eliminates the difference between Hub and PowerDNS DBs on PowerDNS side."),
    ('sync-domains=',
        "The script synchronizes only domains with listed names (separated with comma)."),
    ('exclude-domains=',
        "The script ignores domains with listed names (separated with comma)."),
    ('skip-error-report',
        "The script does not create a report powerdns_diff_report_YYYY-MM-DD_HH-MM-SS.mmm.csv."),
    ('help',
        "The script prints this message and exits (all other arguments are ignored).")
]  # list of available long options


class Color:
    BOLD = '\033[1m'
    END = '\033[0m'


def print_usage():
    print "{}USAGE{}:\n\t{} [--fix-errors [--skip-error-report ...]]\n".format(
        Color.BOLD, Color.END, sys.argv[0]
    )
    print "When the script is called without any parameters, " \
          "it reads data from Hub and PowerDNS databases, " \
          "then compares these data and " \
          "dumps a difference into {}/diff_report_YYYY-MM-DD_HH-MM-SS.mmm.csv.".format(os.getcwd())

    print "\n{}OPTIONS:{}".format(Color.BOLD, Color.END)

    for opt in __long_options:
        print "\t--{}{}{}\n\t\t{}\n".format(Color.BOLD, opt[0].rstrip('='), Color.END, opt[1])


if __name__ == '__main__':
    try:
        opts, args = getopt.getopt(sys.argv[1:], '', dict(__long_options).keys())
        opts = dict(opts)
    except getopt.GetoptError, err:
        print str(err)
        print_usage()
        sys.exit(2)

    if '--help' in opts:
        print_usage()
        sys.exit(0)

    conn = db_client.connect()
    hosts = get_powerdns_hosts(conn)
    if len(hosts) == 0:
        logger.info("No PowerDNS hosts are found.")
        sys.exit(0)

    csv_reporter = PowerDnsSyncCsvReporter(
        "{}/powerdns_diff_report_{}.csv".format(
            os.getcwd(),
            timestamp().replace(':', '-').replace(' ', '_'))
    )

    exclude_domains = opts['--exclude-domains'].split(',') if '--exclude_domains' in opts else None,

    synchronize(
        db_conn=conn,
        csv_reporter=csv_reporter,
        hosts=hosts,
        sync_domains=opts['--sync-domains'].split(',') if '--sync-domains' in opts else None,
        exclude_domains=exclude_domains,
        skip_error_report='--skip-error-report' in opts,
        fix_errors='--fix_errors' in opts,
    )
