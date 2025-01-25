# Stop
docker stop airflow-etl

# Remove
docker rm airflow-etl

# Build
docker build -t airflow-etl .

# Run
docker run -d -p 8080:8080 --name airflow-etl airflow-etl airflow standalone
docker exec -it airflow-etl airflow users create --role Admin --username user --email admin --firstname admin --lastname admin --password 1234

#UI: http://localhost:8080
