import json
import ast
from pymongo import MongoClient
import paho.mqtt.client as mqtt
import urllib.parse

class DBHandler():
    def __init__(self, configuration_file):
        try:
            data = dict()
            with open(configuration_file) as file:
                data = json.load(file)
        except Exception as e:
            print("Error in reading json configuration file: ", e)
            
        self.mqtt_configs = data["MqttConfiguration"]
        
        mongo_db_infos = data["MongoDB"]
        
        if mongo_db_infos["uri"] != "":
            #print("Conecção será com a uri passada")
            password = urllib.parse.quote(mongo_db_infos["uri_password"]) #Usado para transformar os caracteres especias (@; !; etc) da senha no formato correto (%num_correspondente)
            
            uri = mongo_db_infos["uri"]
            uri : str
            uri = uri.replace("<db_password>", password)
            print(uri)
            self.__connection_string = uri
        else: 
            self.__connection_string = "mongodb://{}:{}@{}:{}/?authSource=admin".format(
                mongo_db_infos["USERNAME"],
                mongo_db_infos["PASSWORD"],
                mongo_db_infos["HOST"],
                mongo_db_infos["PORT"]
            )
        
        
        self.__client = None
        self.__db_connection = None
        
        self.__database_name = mongo_db_infos["DB_NAME"]
        
        self._database_collections_names : list[str] = [] 
        self.__collections_connections : dict = {}
        
        devices = data["Devices"]
        
        for device, device_configurations in devices.items():
            device_tags = device_configurations["TagsConfiguration"]
            for tag, tags_configuration in device_tags.items():
                if tags_configuration["Storage"] == True:
                    self._database_collections_names.append(tags_configuration["Topic"])
                    
        #print(self._database_collections_names)
                
        # Dar um jeito de para a aplicação DB caso self.__databese_collections_names ser uma lista vazia        
        
        
        
        #print(self._database_collections_names)
    
    
    def connect_to_db(self):
        try:
            self.__client = MongoClient(self.__connection_string)
            self.__db_connection = self.__client.get_database(self.__database_name)
        
            # Criando um dicionário que conterá como chave o nome de cada coleção e como valor o objeto de conexão
            # desta coleção no banco de dados
            for collection_name in self._database_collections_names:
                self.__collections_connections[collection_name] = self.__db_connection.get_collection(collection_name)
            
            print("Coneções com as coleções: ", self.__collections_connections)
        except Exception as e:
            print("Error in the MongoDb connection: ", e)
        
        
    def insert_document_to_collection(self, collection_name, document):
        collection = self.__collections_connections[collection_name]
        collection.insert_one(document)
        
        #print(f"documento: {document} escrito na coleção {collection} ")
          

class DBClient(DBHandler):
    def __init__(self, configuration_file):
        super().__init__(configuration_file)
        
        
        self.mqtt_client = mqtt.Client()
        
        self.brokerIp = self.mqtt_configs["Ip"]
        self.brokerPort = self.mqtt_configs["Port"]
        
        self.mqtt_client.on_message = self.message_handler
        
        
    def connect_mqtt_client(self):
        try:
            if self.mqtt_client.connect(host=self.brokerIp, port=self.brokerPort, keepalive = 60) == 0: 
                print("Successfully connected to MQTT broker")
                #self.mqttClient.loop_start()
                self.subscribeMqtt()
            else:
                print("Unable to connect to MQTT broker")
        except Exception as e:
            print("Unable to establish connection with the MQTT broker due to error: ", e)
    
    
    def subscribeMqtt(self):
        """Realiza a subscrição em todos os tópicos com storage = true

        """
        #print("\n", self._database_collections_names)
        print("Subscrição realizada nos tópicos: ")
        
        for collection_name in self._database_collections_names:
            self.mqtt_client.subscribe(topic = collection_name)
                
            print(collection_name)
            
    def unsubscribeMqtt(self):
        """Realiza o unsubscribe em todos os tópicos com storage = true

        """
        
        for collection_name in self._database_collections_names:
            self.mqtt_client.unsubscribe(topic = collection_name)
                
                
    def message_handler(self, client, user_data, message):
        try:
            contain = message.payload.decode()
            print(contain)
            print(type(contain))
            #if contain == "":
            #    contain = 0
                
            topico = message.topic
            
            contain = json.loads(contain)
            #contain = json.dumps(contain)
            #contain = ast.literal_eval(contain)
            
            #contain = {
            #    "name": "Luis",
            #    "endereço": "Rua longe",
            #    "pedidos": {
            #        "pizza": 1,
            #        "refrigerante": 2,
            #        "batata": 1,
            #        "hamburguer": 2
            #    }
            #}

            #print(f"Recebido: {contain} do topico {topico}" )
            #print(f"Type de contain = {type(contain)}")
            
            
            self.insert_document_to_collection(collection_name = topico, document = contain)
        except Exception as e:
            print("Erro no inserção da mensagem no banco: ", e)
        
        
    def start(self):
        self.connect_to_db()
        self.connect_mqtt_client()
        self.mqtt_client.loop_start()
    
    def stop(self):
        self.mqtt_client.loop_stop()
        self.unsubscribeMqtt()
        self.mqtt_client.disconnect()
        
    def run(self):
        pass
        
        