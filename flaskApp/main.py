from flask import Flask, jsonify, request, abort
from azure.cosmos import CosmosClient, PartitionKey, exceptions
import json
import os
from flask_marshmallow import Marshmallow
import uuid
from flask_jwt_extended import JWTManager, jwt_required, create_access_token


app = Flask(__name__)


# cosmos db data
url = os.environ['COSMOS_URI']
key = os.environ['COSMOS_KEY']
database_name = 'testPowerPlants'
container_name = 'powerPlants'
admin_container_name = 'admins'


# Initialize the Cosmos client
client = CosmosClient(url, credential=key)
database = client.get_database_client(database_name)
container = database.get_container_client(container_name)
adminContainer = database.get_container_client(admin_container_name)

app.config['JWT_SECRET_KEY'] = str(uuid.uuid4())  #'topSecret' #change to a uuid later
jwt = JWTManager(app)




@app.route("/")
def home():
    return "hello flask!"


@app.route('/powerplants', methods=['GET'])
def getPowerPlants():
    plants = list(container.query_items(
        query="SELECT * from c",
        enable_cross_partition_query=True
    ))
    result = plants_schema.dump(plants)
    return jsonify(result)


@app.route('/powerplants/<name>', methods=['GET'])
def getPowerPlantsByName(name):
    print(name)
    plants = list(container.query_items(
        query=f'SELECT * from c WHERE lower(c.name)=lower(\'{name}\')',
        enable_cross_partition_query=True
    ))

    if len(plants) == 0:
        abort(404)

    plant = plants[0]
    result = plant_schema.dump(plant)
    return jsonify(result)

@app.route('/login', methods=['POST'])
def login():
    if request.is_json:
        username = request.json['username']
        password = request.json['password']
    else:
        abort(402)


    print(username)
    print(password)
    matchingUsernames = list(adminContainer.query_items(
        query=f'SELECT * from c WHERE lower(c.username)=lower(\'{username}\')',
        enable_cross_partition_query=True
    ))
    print(matchingUsernames)

    if len(matchingUsernames) != 1:
        return jsonify(message="incorrect username"), 401

    matchingUsername = matchingUsernames[0]

    print(matchingUsername["password"])

    if matchingUsername["password"] == password:
        access_token = create_access_token(identity=username)
        return jsonify(message="Login succeeded!", access_token=access_token)
    else:
        return jsonify(message="incorrect password"), 401

@app.route('/addpowerplants', methods=['POST'])
@jwt_required()
def addNewPlant():
    if not request.json:
        abort(400)
    powerPlant = request.json
    powerPlantRecord = {**powerPlant, 'id': str(uuid.uuid4())}
    print(powerPlantRecord)
    matchingNames = list(container.query_items(
        query=f'SELECT * from c WHERE lower(c.name)=lower(\'{powerPlantRecord["name"]}\')',
        enable_cross_partition_query=True
    ))
    errors = []

    if len(matchingNames) > 0:
        errors.append('Duplicate names found')

    matchingIds = list(container.query_items(
        query=f'SELECT * from c WHERE lower(c.id)=lower(\'{powerPlantRecord["id"]}\')',
        enable_cross_partition_query=True
    ))

    if len(matchingIds) > 0:
        errors.append("Duplicate IDs found")

    if powerPlantRecord['outputMWH'] < 0:
        errors.append("outputMWH must be greater than 0")

    if powerPlantRecord['coordinates']['lat'] > 90 or powerPlantRecord['coordinates']['lat'] < -90 :
        errors.append("Latitude outside range")
    
    if powerPlantRecord['coordinates']['lon'] > 180 or powerPlantRecord['coordinates']['lon'] < -180 :
        errors.append("Longitude outside  range")

    if len(errors) > 0:
        response = jsonify({'errors': errors})
        response.status_code = 400
        return response

    print(powerPlant)
    dbResult = container.upsert_item(powerPlantRecord)
    result = plant_schema.dump(dbResult)

    return result


# initialize serailizer
ma = Marshmallow(app)


class PlantSchema(ma.Schema):
    class Meta:
        fields = ('id', 'name', 'coordinates',
                  'outputMWH', 'fuelTypes', 'Renewable')


plant_schema = PlantSchema()
plants_schema = PlantSchema(many=True)


if __name__ == '__main__':
    app.run()
