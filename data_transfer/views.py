from django.shortcuts import render
from pymongo import MongoClient
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement
from datetime import datetime
import uuid
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt

@csrf_exempt
def index(request):
    if request.method == 'POST':
        # Get MongoDB and Cassandra connection details from form input
        mongo_uri = request.POST.get('mongo_uri')
        mongo_db_name = request.POST.get('mongo_db')
        mongo_collection_name = request.POST.get('mongo_collection')

        cassandra_host = request.POST.get('cassandra_host')
        cassandra_keyspace = request.POST.get('cassandra_keyspace')
        cassandra_table = request.POST.get('cassandra_table')

        # Validate form data
        if not all([mongo_uri, mongo_db_name, mongo_collection_name, cassandra_host, cassandra_keyspace, cassandra_table]):
            return JsonResponse({'status': 'error', 'message': 'All fields are required.'}, status=400)

        # Connect to MongoDB
        try:
            mongo_client = MongoClient(mongo_uri)
            mongo_db = mongo_client[mongo_db_name]
            mongo_collection = mongo_db[mongo_collection_name]
        except Exception as e:
            return JsonResponse({'status': 'error', 'message': f'MongoDB Connection Error: {str(e)}'}, status=500)

        # Connect to Cassandra
        try:
            cassandra_cluster = Cluster([cassandra_host])
            cassandra_session = cassandra_cluster.connect(cassandra_keyspace)
        except Exception as e:
            return JsonResponse({'status': 'error', 'message': f'Cassandra Connection Error: {str(e)}'}, status=500)

        # Function to map MongoDB types to Cassandra types
        def map_mongo_type_to_cassandra(value):
            if isinstance(value, str):
                return 'text'
            elif isinstance(value, float):
                return 'float'
            elif isinstance(value, int):
                return 'int'
            elif isinstance(value, bool):
                return 'boolean'
            elif isinstance(value, list):
                return 'list<text>'
            elif isinstance(value, datetime):
                return 'timestamp'
            else:
                return 'text'

        # Create the Cassandra table based on MongoDB schema
        sample_doc = mongo_collection.find_one()
        if sample_doc:
            columns = ['uuid uuid']
            for key, value in sample_doc.items():
                if key == "_id":
                    continue
                cassandra_type = map_mongo_type_to_cassandra(value)
                columns.append(f"{key} {cassandra_type}")

            create_table_query = f"CREATE TABLE IF NOT EXISTS {cassandra_table} ({', '.join(columns)}, PRIMARY KEY (uuid));"
            cassandra_session.execute(create_table_query)

            # Transfer data
            for doc in mongo_collection.find():
                unique_uuid = uuid.uuid4()
                filtered_doc = {k: v for k, v in doc.items() if k != '_id'}
                filtered_doc['uuid'] = unique_uuid

                columns = ', '.join(filtered_doc.keys())
                placeholders = ', '.join(['%s'] * len(filtered_doc))
                insert_query = f"INSERT INTO {cassandra_table} ({columns}) VALUES ({placeholders})"
                values = [filtered_doc[k] for k in filtered_doc]

                prepared_stmt = SimpleStatement(insert_query)
                cassandra_session.execute(prepared_stmt, values)

            return render(request, 'success.html')

    return render(request, 'index.html')

def get_collections(request):
    mongo_uri = request.GET.get('mongo_uri')
    mongo_db = request.GET.get('mongo_db')

    if not mongo_uri or not mongo_db:
        return JsonResponse({"error": "MongoDB URI and database name are required"}, status=400)

    try:
        client = MongoClient(mongo_uri)
        db = client[mongo_db]
        collections = db.list_collection_names()
        return JsonResponse({"collections": collections})
    except Exception as e:
        return JsonResponse({"error": str(e)}, status=500)

@csrf_exempt
def confirm_transfer(request):
    if request.method == 'POST':
        # Get MongoDB and Cassandra connection details from form input
        mongo_uri = request.POST.get('mongo_uri')
        mongo_db_name = request.POST.get('mongo_db')
        mongo_collection_name = request.POST.get('mongo_collection')

        cassandra_host = request.POST.get('cassandra_host')
        cassandra_keyspace = request.POST.get('cassandra_keyspace')
        cassandra_table = request.POST.get('cassandra_table')

        # Connect to MongoDB
        mongo_client = MongoClient(mongo_uri)
        mongo_db = mongo_client[mongo_db_name]
        mongo_collection = mongo_db[mongo_collection_name]

        # Connect to Cassandra
        cassandra_cluster = Cluster([cassandra_host])
        cassandra_session = cassandra_cluster.connect(cassandra_keyspace)

        print("request post ::::: %s" %request.POST)

        # Function to map MongoDB types to Cassandra types
        def map_mongo_type_to_cassandra(value):
            if isinstance(value, str):
                return 'text'
            elif isinstance(value, float):
                return 'float'
            elif isinstance(value, int):
                return 'float'
            elif isinstance(value, bool):
                return 'boolean'
            elif isinstance(value, list):
                return 'list<text>'
            elif isinstance(value, datetime):
                return 'timestamp'
            else:
                return 'text'

        # Create the Cassandra table based on MongoDB schema
        sample_doc = mongo_collection.find_one()
        if sample_doc:
            columns = ['uuid uuid']
            for key, value in sample_doc.items():
                if key == "_id":
                    continue
                cassandra_type = map_mongo_type_to_cassandra(value)
                columns.append(f"{key} {cassandra_type}")
            
            create_table_query = f"CREATE TABLE IF NOT EXISTS {cassandra_table} ({', '.join(columns)}, PRIMARY KEY (uuid));"
            print(columns)
            
            cassandra_session.execute(create_table_query)

            # Transfer data
            for doc in mongo_collection.find():
                unique_uuid = uuid.uuid4()
                filtered_doc = {k: v for k, v in doc.items() if k != '_id'}
                filtered_doc['uuid'] = unique_uuid

                columns = ', '.join(filtered_doc.keys())
                placeholders = ', '.join(['%s'] * len(filtered_doc))
                insert_query = f"INSERT INTO {cassandra_table} ({columns}) VALUES ({placeholders})"
                values = [filtered_doc[k] for k in filtered_doc]

                prepared_stmt = SimpleStatement(insert_query)
                cassandra_session.execute(prepared_stmt, values)

            return render(request, 'success.html')
        
    return render(request, 'index.html')



def get_schema(request):
    mongo_uri = request.GET.get('mongo_uri')
    mongo_db = request.GET.get('mongo_db')
    mongo_collection = request.GET.get('mongo_collection')

    try:
        client = MongoClient(mongo_uri)
        db = client[mongo_db]
        collection = db[mongo_collection]
        sample_doc = collection.find_one()

        if not sample_doc:
            return JsonResponse({"schema": []})

        schema = [{"name": key, "type": type(value).__name__} for key, value in sample_doc.items() if key != "_id"]
        return JsonResponse({"schema": schema})
    except Exception as e:
        return JsonResponse({"error": str(e)}, status=500)