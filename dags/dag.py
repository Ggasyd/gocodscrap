import os
import logging
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from airflow.hooks.base_hook import BaseHook
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from google.cloud import storage
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator
from airflow.operators.bash_operator import BashOperator
import pyarrow.csv as pv
import pyarrow.parquet as pq
import requests
from airflow.utils.email import send_email
from airflow.utils.trigger_rule import TriggerRule
import time

# Variables nécessaires
SLACK_CONN_ID = 'slack_connection_id'

slack_webhook_token = BaseHook.get_connection(SLACK_CONN_ID).password
channel = BaseHook.get_connection(SLACK_CONN_ID).login
   


def send_success_slack_message():
    slack_webhook_token = BaseHook.get_connection(SLACK_CONN_ID).password
    channel = BaseHook.get_connection(SLACK_CONN_ID).login
    print(slack_webhook_token)
    print(channel)
    slack_msg = "Sucessful jobs"

    slack_alert = SlackWebhookOperator(
        task_id='send_slack_message',
        slack_webhook_conn_id=SLACK_CONN_ID,
        #webhook_token=slack_webhook_token,
        message=slack_msg,
        channel=channel,
        username='airflow'
    )

    slack_alert.execute({})


def send_failed_slack_message():
    slack_webhook_token = BaseHook.get_connection(SLACK_CONN_ID).password
    channel = BaseHook.get_connection(SLACK_CONN_ID).login
    print(slack_webhook_token)
    print(channel)
    slack_msg = "Failed jobs"

    slack_alert = SlackWebhookOperator(
        task_id='send_slack_message',
        slack_webhook_conn_id=SLACK_CONN_ID,
        #webhook_token=slack_webhook_token,
        message=slack_msg,
        channel=channel,
        username='airflow'
    )

    slack_alert.execute({})


# VARIABLES
GITHUB_TOKEN = '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'
PROJECT_ID = "gocod-412807"
BUCKET = "gocod"
dataset_file = "winequality-red.csv"
dataset_url = f"https://archive.ics.uci.edu/ml/machine-learning-databases/repo_architecture/"
path_to_local_home = "/opt/airflow/stockage/" # we download in this location so file is is not deleted when the downloading task finishes
parquet_file = dataset_file.replace('.csv', '.parquet')
BIGQUERY_DATASET = 'winequality'
# Configure GCS client
client = storage.Client()
bucket = client.bucket(BUCKET)
    
# Adjust settings to prevent timeout for large files (workaround)
storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB
 



def send_success_email(context):
    subject = f"Success: {context['task_instance_key_str']}"
    html_content = f"""
    <h3>Task Success</h3>
    Task {context['task_instance_key_str']} succeeded.
    """
    send_email('maczibay@gmail.com', subject, html_content)

def send_failure_email(context):
    subject = f"Failure: {context['task_instance_key_str']}"
    html_content = f"""
    <h3>Task Failure</h3>
    Task {context['task_instance_key_str']} failed.
    """
    send_email('maczibay@gmail.com', subject, html_content)


def fetch_contents(owner, repo, path='', depth=0, file_path='/opt/airflow/stockage/', file=None, delay=1):
    """
    Fonction récursive pour récupérer et afficher le contenu des dossiers GitHub sous forme d'arbre,
    et écrire les résultats dans un fichier texte spécifié par l'utilisateur.
    :param owner: Propriétaire du dépôt.
    :param repo: Nom du dépôt.
    :param path: Chemin relatif à explorer dans le dépôt.
    :param depth: Profondeur actuelle dans l'arbre de répertoire.
    :param file_path: Chemin du fichier dans lequel écrire les résultats.
    :param file: Objet fichier dans lequel écrire, utilisé lors des appels récursifs.
    :param delay: Délai initial pour la politique d'exponential backoff.
    """
    if depth > 600:  # Vérifier si la profondeur dépasse la limite autorisée
        print("Limite de profondeur atteinte, passage à l'élément suivant.")
        return  # Arrêter l'exécution pour cette branche

    url = f'https://api.github.com/repos/{owner}/{repo}/contents/'
    headers = {"Authorization": f"token {GITHUB_TOKEN}"}
    full_url = f"{url}{path}" if path else url
    response = requests.get(full_url, headers=headers)
    
    if response.status_code == 200:
        contents = response.json()
        file_path = file_path if file else file_path + "Architecture_" + str(repo) + ".txt"
        
        if file is None:
            with open(file_path, 'w') as file:
                # Réexécution avec le fichier ouvert
                fetch_contents(owner, repo, path, depth, file_path, file, delay)
        else:
            indent = '    ' * depth
            for item in contents:
                line = f"{indent}- {item['name']} ({item['type']})\n"
                file.write(line)
                if item['type'] == 'dir':
                    fetch_contents(owner, repo, item['path'], depth + 1, file_path, file, delay)
    elif response.status_code == 403:
        # Gestion de la limite de taux
        rate_limit_remaining = int(response.headers.get('X-RateLimit-Remaining', 0))
        if rate_limit_remaining == 0:
            rate_limit_reset = int(response.headers.get('X-RateLimit-Reset', 0))
            reset_timeout = max(rate_limit_reset - time.time(), 0) + delay
            print(f"Limite de taux atteinte, mise en pause pour {reset_timeout} secondes.")
            time.sleep(reset_timeout)
            fetch_contents(owner, repo, path, depth, file_path, file, delay * 2)
    else:
        print(f"Erreur lors de la récupération du contenu: {response.status_code}")       

def repo_recuperation():
    url = "https://api.github.com/search/repositories"
    url += "?q=cookiecutter"
    headers = {
    "Accept": "application/vnd.github.v3+json",
    "Authorization": f"token {GITHUB_TOKEN}"
    }
    base_url = "https://api.github.com/search/repositories"
    # Ajoutez les dates dans le format AAAA-MM-JJ
    search_query = "cookiecutter+created:2009-01-01..2016-01-01"

    all_items = []
    page = 1
    per_page = 100


    while True:
        paged_url = f"{base_url}?q={search_query}&page={page}&per_page={per_page}"
        res = requests.get(paged_url, headers=headers)
        
        # Vérifiez le code de statut
        if res.status_code != 200:
            print(f"Failed to fetch data: Status code {res.status_code}, Response: {res.json()}")
            break
        
        # Convertissez la réponse en JSON et récupérez les éléments
        response_json = res.json()
        items = response_json.get('items', [])
        
        # Vérifiez si des éléments ont été récupérés
        if not items:
            # Si aucun élément n'est retourné, alors nous avons atteint la fin
            break
        
        # Ajoutez les éléments à la liste de tous les items
        all_items.extend(items)
        
        # Affichez le progrès
        print(f"Page: {page}, Total items fetched: {len(all_items)}")
        
        # Passez à la page suivante
        page += 1
        
        # Attendez 5 secondes avant la prochaine requête pour respecter les limites de l'API
        #time.sleep(5)

    print(f"Total items fetched: {len(all_items)}")

    search_base_query = "cookiecutter+created:{start_date}..{end_date}"

    repos = []

    # Définissez la plage d'années pour la requête
    start_year = 2016
    end_year = 2024

    for year in range(start_year, end_year):
        # Définissez les dates de début et de fin pour chaque année
        start_date = f"{year}-01-02"
        end_date = f"{year+1}-01-01"
        
        # Mettez à jour la requête de recherche avec les dates actuelles
        search_query = search_base_query.format(start_date=start_date, end_date=end_date)
        page = 1
        per_page = 100

        while True:
            paged_url = f"{base_url}?q={search_query}&page={page}&per_page={per_page}"
            res = requests.get(paged_url, headers=headers)
            
            # Vérifiez le code de statut
            if res.status_code != 200:
                print(f"Failed to fetch data for {year}: Status code {res.status_code}, Response: {res.json()}")
                break
            
            # Convertissez la réponse en JSON et récupérez les éléments
            response_json = res.json()
            items = response_json.get('items', [])
            
            # Vérifiez si des éléments ont été récupérés
            if not items:
                # Si aucun élément n'est retourné, alors nous avons atteint la fin pour l'année en cours
                break
            
            # Ajoutez les éléments à la liste de tous les items
            repos.extend(items)
            
            # Affichez le progrès
            print(f"Year: {year}, Page: {page}, Items fetched this year: {len(items)}, Total items fetched: {len(repos)}")
            
            # Passez à la page suivante
            page += 1
            
            # Attendez 5 secondes avant la prochaine requête pour respecter les limites de l'API
            time.sleep(5)

    search_base_query = "cookiecutter+created:{start_date}..{end_date}"

    repositories = []  # Changement de nom de la variable ici

    # Années pour lesquelles vous voulez récupérer les données mois par mois
    years_with_monthly_query = [2019, 2020, 2021, 2022, 2023]

    for year in years_with_monthly_query:
        for month in range(1, 13):  # De janvier (1) à décembre (12)
            start_date = f"{year}-{month:02d}-01"
            if month == 12:
                end_date = f"{year+1}-01-01"  # Si décembre, la fin est le début de l'année suivante
            else:
                end_date = f"{year}-{month+1:02d}-01"  # Sinon, la fin est le début du mois suivant

            search_query = search_base_query.format(start_date=start_date, end_date=end_date)
            page = 1
            per_page = 100

            while True:
                paged_url = f"{base_url}?q={search_query}&page={page}&per_page={per_page}"
                res = requests.get(paged_url, headers=headers)

                if res.status_code != 200:
                    print(f"Failed to fetch data for {year}-{month:02d}: Status code {res.status_code}, Response: {res.json()}")
                    break

                response_json = res.json()
                items = response_json.get('items', [])
                
                if not items:
                    break

                repositories.extend(items)  # Changement de nom de la variable ici aussi
                print(f"Year: {year}, Month: {month}, Page: {page}, Items fetched this period: {len(items)}, Total items fetched: {len(repositories)}")
                page += 1
                time.sleep(2)  # Décommentez pour respecter les limites de l'API

    print(f"Total items fetched: {len(repositories)}")  # Et ici
    
    combined_list = repositories

    # Utiliser un dictionnaire pour éliminer les doublons en se basant sur l'ID unique
    unique_repos_dict = {repo['id']: repo for repo in combined_list}

    # Convertir le dictionnaire en liste pour obtenir la liste finale des dépôts uniques
    unique_repos_list = list(unique_repos_dict.values())

    print(f"Total unique repositories: {len(unique_repos_list)}")
#    for repository in unique_repos_list:
#        owner = repository["owner"]["login"]
#        repo = repository["name"]
#        print(repo)
#        fetch_contents(owner, repo)

    for filename in os.listdir(path_to_local_home):
        if filename.endswith(".txt"):  # Only upload .txt files
            local_file_path = os.path.join(path_to_local_home, filename)
            
            # Create a blob object in the bucket
            blob = bucket.blob(filename)
            
            # Upload the file to GCS
            blob.upload_from_filename(local_file_path)
            
            print(f"File {filename} uploaded to GCS bucket {BUCKET}.")




default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
    "depends_on_past": False,
    "retries": 1,
}


with DAG(
    dag_id="data_ingestion_from_github",
    schedule_interval="@daily", # https://airflow.apache.org/docs/apache-airflow/1.10.1/scheduler.html
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    tags=['dtc-de'],
) as dag:


    repo_recuperation_task = PythonOperator(
        task_id="repo_recuperation",
        python_callable=repo_recuperation,
        dag=dag,
    )


    slack_success_message = SlackWebhookOperator(
        task_id='send_suceesful_slack_message',
        slack_webhook_conn_id=SLACK_CONN_ID,
        #webhook_token=slack_webhook_token,
        message="Success: Job completed successfully",
        channel=channel,
        username='airflow',
        trigger_rule=TriggerRule.ALL_SUCCESS  # Déclencher en cas de succès de toutes les tâches précédentes
    )

    slack_failure_message = SlackWebhookOperator(
        task_id='send_failed_slack_message',
        slack_webhook_conn_id=SLACK_CONN_ID,
        #webhook_token=slack_webhook_token,
        message="Failure: Job failed",
        channel=channel,
        username='airflow',
        trigger_rule=TriggerRule.ONE_FAILED  # Déclencher si au moins une tâche précédente a échoué
    )

    # Workflow for task direction
    repo_recuperation_task >> [slack_success_message, slack_failure_message]

