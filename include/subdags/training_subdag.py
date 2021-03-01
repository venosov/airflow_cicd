from airflow import DAG
from airflow.providers.papermill.operators.papermill import PapermillOperator

def subdag_factory(parent_dag_id, child_dag_id, default_args):
    with DAG(dag_id=f"{parent_dag_id}.{child_dag_id}", default_args=default_args) as dag:

        n_estimators = [100, 150]
        max_features = ['auto','sqrt']

        training_model_tasks = []
        for feature in max_features:
            for estimator in n_estimators:
                ml_id = f"{feature}_{estimator}"
                training_model_tasks.append(PapermillOperator(
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
                ))
        return dag