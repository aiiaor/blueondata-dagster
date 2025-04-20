from dagster import resource

@resource
def postgres_resource(init_context):
    import psycopg2
    conn = psycopg2.connect(
        host="localhost",
        port=5433,
        user="datauser",
        password="datapass",
        dbname="datawarehouse"
    )
    return conn