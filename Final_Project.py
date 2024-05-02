import redis
import csv


#Redis info
redis_host = 'redis-19859.c282.east-us-mz.azure.redns.redis-cloud.com'
redis_port = 19859
redis_password = 'prQ3OqV7CAiYKRFEO8nkXC2bGFcFYRG4'
redis_dict = {}
redis_client = redis.Redis(host=redis_host, port=redis_port, password=redis_password)

csv_data = "metacritic_games.csv"

# Chunk the data in the CSV file
with open(csv_data, 'r') as csv_file:
    csv_reader = csv.reader(csv_file)
    headers = next(csv_reader) 
    chunk_size = 1000

    pipeline = redis_client.pipeline()

    for i, row in enumerate(csv_reader):
        key = row[0]
        values = row[1:]

        #Insert data into Redis
        for j in range(len(values)):
            print("DATA: " + str(j))
            pipeline.hset(key, headers[j + 1], values[j])

    #Execute pipeline every chunk_size iterations
        if (i + 1) % chunk_size == 0:
            pipeline.execute()
            pipeline = redis_client.pipeline()

        #Execute remaining commands in the pipeline
        pipeline.execute()

    print("CSV data loaded into Redis successfully.")