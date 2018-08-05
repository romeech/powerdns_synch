def _make_quoted_csv_str(lst):
    return ','.join(["'{}'".format(x) for x in lst])


class PowerDnsSqlReference(object):
    @staticmethod
    def select_dns_records(domain_ids_list):
        domains_str = ','.join(domain_ids_list)
        sql = """SELECT t.id,
                        t.idn_host,
                        t.type,
                        t.rec_data,
                        t.ttl,
                        t.prio,
                        t.domain_id,
                        (SELECT name FROM domains WHERE id = t.domain_id) as domain_name,
                        md5(t.idn_host || t.type || t.rec_data) AS rec_hash,
                        md5(t.idn_host || t.type || t.rec_data || t.ttl) AS ttl_hash
                 FROM
                   (SELECT id,
                           trim(name) AS idn_host,
                           trim(type) AS type,
                           prio || ' ' || trim(content) AS rec_data,
                           ttl,
                           prio,
                           domain_id
                    FROM records
                    WHERE type IN ('SRV',
                                   'MX')
                      AND domain_id in ({0})
                    UNION ALL SELECT id,
                                     trim(name) AS idn_host,
                                     trim(type) AS type,
                                     trim(content) AS rec_data,
                                     ttl,
                                     0 AS prio,
                                     domain_id
                    FROM records
                    WHERE type IN ('A',
                                   'TXT',
                                   'NS',
                                   'CNAME',
                                   'AAAA',
                                   'PTR',
                                   'NAPTR')
                    AND domain_id in ({1})) AS t
            ORDER BY t.id DESC""".format(domains_str, domains_str)
        return sql

    @staticmethod
    def select_domains(sync_domains=None, exclude_domains=None):
        sql = """SELECT id AS domain_id, trim(name) AS domain_name FROM domains"""
        prefix = "WHERE"
        if sync_domains:
            sql += """ {0} trim(name) IN ({1})""".format(prefix, _make_quoted_csv_str(sync_domains))
            prefix = "AND"
        if exclude_domains:
            sql += """ {0} trim(name) NOT IN ({1})""".format(
                prefix, _make_quoted_csv_str(exclude_domains)
            )
            prefix = "AND"
        return sql

    @staticmethod
    def delete_dns_records(id_list):
        return """DELETE FROM records WHERE id IN ({0})""".format(','.join(id_list))

    @staticmethod
    def update_dns_records(ttl_map):
        sql_template = "UPDATE records SET ttl = {} WHERE id = {}"
        return ';'.join(sql_template.format(ttl, rec_id) for (rec_id, ttl) in ttl_map.items())


class HubSqlReference(object):
    @staticmethod
    def select_powerdns_hosts():
        sql = """SELECT host_id
                 FROM registered_hosts
                 WHERE primary_name IN
                     (SELECT da.name
                      FROM dns_agents da
                      INNER JOIN dns_agent_type dat ON da.plugin_id = dat.plugin_id
                      WHERE dat.name = 'PowerDns')"""
        return sql

    @staticmethod
    def select_domain_id(domain_names):
        domain_names_str = _make_quoted_csv_str(domain_names)
        sql = """SELECT id
                 FROM domains
                 WHERE trim(name) IN ({})
                 AND state = 'g'
                 AND type IN ('d', 's')""".format(domain_names_str)
        return sql

    @staticmethod
    def select_dns_records(domain_ids_list):
        domains_str = ','.join(map(str, domain_ids_list))
        sql = """SELECT rr.rr_type,
                        rr.rec_data,
                        rr.ttl,
                        md5(rr.idn_host || rr.rr_type || rr.rec_data) AS rec_hash,
                        md5(rr.idn_host || rr.rr_type || rr.rec_data || ttl) AS ttl_hash
                  FROM
                   (SELECT trim(drr.rr_type) AS rr_type,
                           trim(trailing '.' from trim(drr.idn_host)) as idn_host,
                           CASE
                               WHEN drr.rr_type IN ('CNAME',
                                                    'PTR',
                                                    'SRV',
                                                    'NS',
                                                    'MX')
                                    THEN trim(trailing '.' from trim(drr.idn_data))
                               ELSE trim(drr.idn_data)
                           END AS rec_data,
                           CASE
                               WHEN drr.ttl = 0 THEN dsr.min_ttl
                               ELSE drr.ttl
                           END AS ttl,
                           drr.domain_id
                    FROM dns_resource_records drr
                    INNER JOIN domains d ON d.domain_id = drr.domain_id
                    INNER JOIN dns_sys_records dsr ON d.sys_record_id = dsr.record_id) AS rr
                  WHERE rr.domain_id IN ({0})""".format(domains_str)

        # get system NS records
        sql += """ UNION ALL """
        sql += """SELECT t.rrtype,
                         t.rec_data,
                         t.ttl,
                         md5(t.idn_host || t.rrtype || t.rec_data) AS rec_hash,
                         md5(t.idn_host || t.rrtype || t.rec_data || t.ttl) AS ttl_hash
                  FROM
                    (SELECT 'NS'::text AS rrtype,
                            trim(dsr.ns1_name) AS rec_data,
                            dsr.min_ttl AS ttl,
                            d.domain_id,
                            trim(d.name) as idn_host
                     FROM dns_sys_records dsr
                     INNER JOIN domains d ON d.sys_record_id = dsr.record_id
                     WHERE dsr.ns1_name IS NOT NULL
                     UNION SELECT 'NS'::text AS rrtype,
                                  trim(dsr.ns2_name) AS rec_data,
                                  dsr.min_ttl AS ttl,
                                  d.domain_id,
                                  trim(d.name) as idn_host
                     FROM dns_sys_records dsr
                     INNER JOIN domains d ON d.sys_record_id = dsr.record_id
                     WHERE dsr.ns2_name IS NOT NULL
                     UNION SELECT 'NS'::text AS rrtype,
                                  trim(dsr.ns3_name) AS rec_data,
                                  dsr.min_ttl AS ttl,
                                  d.domain_id,
                                  trim(d.name) as idn_host
                     FROM dns_sys_records dsr
                     INNER JOIN domains d ON d.sys_record_id = dsr.record_id
                     WHERE dsr.ns3_name IS NOT NULL) AS t
                  WHERE t.domain_id IN ({0})""".format(domains_str)

        return sql
