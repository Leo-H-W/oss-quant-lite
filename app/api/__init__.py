from flask import Blueprint

api_bp = Blueprint('api', __name__)

from . import stock_api, analysis_api, text2sql_api, trial_api  # noqa: F401
from .workbench_api import workbench_bp  # noqa: F401