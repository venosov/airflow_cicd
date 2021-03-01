from airflow.utils.task_group import TaskGroup
from airflow.providers.papermill.operators.papermill import PapermillOperator
from airflow.models import DAG, Variable

def training_groups():
    with TaskGroup("trainings") as group:

        model_settings = Variable.get('avocado_dag_model_settings', deserialize_json=True)

        for feature in model_settings['max_features']:
            for estimator in model_settings['n_estimators']:
                ml_id = f"{feature}_{estimator}"
                PapermillOperator(
                    task_id=f'training_model_{ml_id}',
                    input_nb='/usr/local/airflow/include/notebooks/avocado_prediction.ipynb',
                    output_nb=f'/tmp/out-model-avocado-prediction-{ml_id}.ipynb',
                    pool='training_pool',
                    parameters={
                        'filepath': '/tmp/avocado.csv',
                        'n_estimators': estimator,
                        'max_features': feature,
                        'ml_id': ml_id
                    }
                )
        return group