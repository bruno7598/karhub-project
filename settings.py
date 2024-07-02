import os

import yaml
from easydict import EasyDict as edict
from modules.singleton import Singleton


class Settings(metaclass=Singleton):
    def __init__(self):
        pass

    def set_env(self, env):
        """
        Set the environment settings based on the specified environment.

        Args:
            env (str): The name of the desired environment.

        Returns:
            None
        """
        with open(os.path.dirname(__file__) + "/config/environment.yaml") as sc:
            config = yaml.safe_load(sc)
            self.__dict__.update(edict(config[env]))
        self.env = env
