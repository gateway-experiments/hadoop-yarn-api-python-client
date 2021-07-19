import requests

class SimpleAuth(requests.auth.AuthBase):
    def __init__(self, username):
        self.username = username
        self.auth_token = None
        self.auth_done = False

    def __call__(self, request):
        if not self.auth_done:
            _session = requests.Session()
            r = _session.get(request.url, params={"user.name": self.username})
            r.raise_for_status()
            self.auth_token = _session.cookies.get_dict()['hadoop.auth']
            self.auth_done = True
        else:
            request.cookies.set("hadoop.auth", self.auth_token)
        return request
