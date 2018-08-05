import csv


class CsvReporter:
    def __init__(self, path):
        self._path = path
        self._csv_file = open(path, 'wb')
        self._field_names = []
        self._writer = csv.writer(self._csv_file)

    def set_field_names(self, field_set):
        self._field_names = field_set

    def post_header(self):
        self._writer.writerow(self._field_names)

    def post_row(self, values):
        values_len = len(values)
        header_len = len(self._field_names)

        if values_len != header_len:
            exc_msg = "CsvReporter: row fields does not match header!" \
                " Expected {0} fields, passed {1} fields"
            raise Exception(exc_msg.format(header_len, values_len))

        self._writer.writerow([unicode(s).encode("utf-8") for s in values])

    def close(self):
        self._csv_file.close()

    def get_report_path(self):
        return self._path

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()
        return self


class PowerDnsSyncCsvReporter(CsvReporter):
    # Fields
    FLD_HOST_ID = "Host ID"
    FLD_DOMAIN_ID = "Domain ID"
    FLD_DOMAIN_NAME = "Domain Name"
    FLD_RECORD_ID = "Record ID"
    FLD_RR_TYPE = "Resource Record Type"
    FLD_CONTENT = "Content"
    FLD_TTL = "TTL"
    FLD_PRIO = "Priority"
    FLD_ERROR_TYPE = "Error"
    FLD_SUGGESTED_FIX = "Suggested Fix"

    def __init__(self, path):
        CsvReporter.__init__(self, path)

        # Order
        self.set_field_names([self.FLD_HOST_ID,
                              self.FLD_DOMAIN_ID,
                              self.FLD_DOMAIN_NAME,
                              self.FLD_RECORD_ID,
                              self.FLD_RR_TYPE,
                              self.FLD_CONTENT,
                              self.FLD_TTL,
                              self.FLD_PRIO,
                              self.FLD_ERROR_TYPE,
                              self.FLD_SUGGESTED_FIX])

    def post_update_required(
        self, host_id, domain_id, domain_name, rec_id, rr_type, content, prio, old_ttl, new_ttl
    ):
        values = [host_id, domain_id, domain_name, rec_id, rr_type, content, old_ttl, prio,
                  "Record TTL is out-dated",
                  "Set TTL to {0} in PowerDNS database.".format(new_ttl)]
        self.post_row(values)

    def post_remove_duplicate(
        self, host_id, domain_id, domain_name, rec_id, rr_type, content, prio, ttl
    ):
        values = [host_id, domain_id, domain_name, rec_id, rr_type, content, ttl, prio,
                  "Duplicate record",
                  "Remove the record from PowerDNS database."]
        self.post_row(values)
