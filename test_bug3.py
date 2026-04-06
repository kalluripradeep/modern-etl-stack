from functools import cached_property

class TestException(Exception): pass

class Manager:
    def __init__(self):
        self.appbuilder = None
        
    @cached_property
    def security_manager(self):
        if not self.appbuilder:
            raise TestException("AppBuilder missing!")
        return "SM"

m = Manager()
try:
    hasattr(m, "security_manager")
    print("Passed hasattr")
except Exception as e:
    print(f"Failed hasattr: {type(e)}")
