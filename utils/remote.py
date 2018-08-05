class Request(object):
    """a stub for remote calls"""
    def __init__(self, host_id, login, password):
        super(Request, self).__init__()
        self.host_id = host_id
        self.login = login
        self.password = password

    def command(self, command_body, valid_exit_codes, stdout, stderr, retvar):
        pass

    def perform(self):
        return {
            'retcode': '0',
            'stdout': '',
            'stderr': '',
        }
