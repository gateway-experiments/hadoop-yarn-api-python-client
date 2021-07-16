import requests

class SimpleAuth(requests.auth.AuthBase):
    def __init__(self, username):
        self.username = username
        self.auth_done = False

    def __call__(self, request):
        if not self.auth_done:
            r = requests.get(request.url, params={"user.name": self.username})
            r.raise_for_status()
            self.auth_done = True
        return request
