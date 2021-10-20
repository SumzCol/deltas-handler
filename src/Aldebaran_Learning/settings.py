# -*- coding: utf-8 -*-
from Aldebaran_Learning.hooks import ProjectHooks

from .sparkContext import CustomContext

# Instantiate and list your project hooks here
HOOKS = (ProjectHooks(),)

# Define custom context class. Defaults to `KedroContext`
CONTEXT_CLASS = CustomContext

# Define the configuration folder. Defaults to `conf`
# CONF_ROOT = "conf"
