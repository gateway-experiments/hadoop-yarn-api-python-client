import requests

class SimpleAuth(requests.auth.AuthBase):
    def __init__(self, username="yarn"):
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

        # Borrowed from https://github.com/psf/requests/issues/2532#issuecomment-90126896
        if 'Cookie' in request.headers:
            old_cookies = request.headers['Cookie']
            all_cookies = '; '.join([old_cookies, "{0}={1}".format("hadoop.auth", self.auth_token)])
            request.headers['Cookie'] = all_cookies
        else:
            request.prepare_cookies({"hadoop.auth": self.auth_token})
        return request
