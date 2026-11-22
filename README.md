# Shopping Sessions

An amd64 container for Databricks Shopping Sessions, scorer version shopping-sessions-5. The image reads the mounted dataset and writes the user and campaign daily Parquet tables.

```sh
docker buildx build --platform linux/amd64 --load -t shopping-sessions .
```
