# powerdns_sync
An utility to synchronise DNS records in PowerDNS database using Management Hub as etalon.

There is Hub server with UI for managing DNS records and a database for storing the settings.

There are also several nodes with [PowerDNS](https://en.wikipedia.org/wiki/PowerDNS) instances on it. PowerDNS has own database where it stores DNS-records.

When a user changes DNS-records in UI of the Hub the adjustments are propagated to all PowerDNS nodes. Some failures (like networking, bugs in Hub server code) lead to consistency violation between Hub and PowerDNS databases. Here is the list of possible violations:
* Records deleted in Hub are remained at least in one of the PowerDNS DB
* There are several records in PowerDNS differ only by TTL
* There is a record in PowerDNS has a different TTL then in Hub

This utility is meant to display (in CSV) such inconsistencies and optionally fix them automatically (--fix-errors option).

Run help for details:
``python powerdns_sync.py --help``

