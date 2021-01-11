class ForwardContract():
    def __init__(self, call, put):
        self.call = call
        self.put = put
        self.strike = call.strike
        self.exercise = call.exercise
        self.expires = call.expires

    def __str__(self):
        return f'{self.strike}@{self.exercise}'


