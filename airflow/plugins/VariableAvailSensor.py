from airflow.sensors.base import BaseSensorOperator
from airflow.utils.decorators import apply_defaults
from airflow.models import Variable

class VariableAvailSensor(BaseSensorOperator):
    @apply_defaults
    def __init__(self, varnames, *args, **kwargs):
        super(VariableAvailSensor, self).__init__(*args, **kwargs)
        
        self.varnames = varnames

    
    def poke(self, context):

        status = True
        for var in self.varnames:
            variable = Variable.get(var, default_var=None)
            if variable == None:
                status = False

        return status




