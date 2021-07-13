
# Infrastructure-as-code
install-infra:
	python3 -m venv venv/infra
	venv/infra/bin/pip install -r requirements.txt

create-cluster: install-infra
	venv/infra/bin/python dags/scripts/infrastructure/create_cluster.py

delete-cluster:
	venv/infra/bin/python dags/scripts/infrastructure/delete_cluster.py

# Airflow
image:
	docker build . -f Dockerfile --tag lpillmann/airflow:0.0.1

airflow:
	docker-compose up

stop-all-containers:
	docker stop $(docker ps -a -q)