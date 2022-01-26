from airflow.sensors.base import BaseSensorOperator
from airflow.utils.decorators import apply_defaults
from airflow.models import Variable

class VariableAvailSensor(BaseSensorOperator):
    @apply_defaults
    def __init__(self, varname, *args, **kwargs):
        self.varname = varname

    
    def poke(self, context):
        variable = Variable.get(self.name, default_var=None)

        return True if variable else False




