import json
from Modbus.ModbusClient import ClienteModbus 
import paho.mqtt.client as mqtt
import threading
from datetime import datetime
import time

class ModbusMqttGateway():
    def __init__(self, path_to_configuration_file, **kw):
        try:
            data = dict()
            with open(path_to_configuration_file) as file:
                data = json.load(file)
        except Exception as e:
            print("Error in reading json configuration file: ", e)
        
        #Adquirindo as informações de configuração MQTT
        self.mqttData = data["MqttConfiguration"]
        
        self.brokerIp = self.mqttData["Ip"]
        self.brokerPort = self.mqttData["Port"]
        
        self.devicesData = data["Devices"]
        #print(self.devicesData)
        
        self.devices = list(self.devicesData.keys())
        self.num_devices = len(self.devices)
        
        self.devicesTags = []
        self.devicesTags : list[dict]            #Indicando que será uma lista que conterá em cada indice as configuratiosTags como dicionários
        
        #self.topics = []
        #self.topics : list[dict]                 #Lista que cada indice será um dispositivo e conterá um dicionário com os tópicos de cada tag como chave de um dicionário 
                                                 #com os resto das informações da tag
                                                 
        self.topics = []
        self.topics : list[list[dict]]                                           
        
        self.identification = []                 #Cada elemento será uma tupla que conterá na primeira posição o indice dos clientes modbus iniciados com o mesmo IP
                                                 #e o seundo elemento será o indice para obter o client modbus iniciado com o unit_id especificado
                                                 #só é possível utilizar isto pois as tags estarão ordenadas, e terá uma matriz onde cada linha (elemento da lista externa)
                                                 #conterá clientes modbus com o mesmo ip e cada coluna clientes modbus com unit_id diferente. 
                                                 #Haverá também uma matriz em que cada linha apresentará os topicos (tags) para o cliente com mesmo ip e cada coluna será
                                                 #os tópicos para o cliente de um unit_id específico. Com isso, a variavel self.identification será usada para relacionar
                                                 #e comunicar estas duas matriz (self.modbusClients e self.topics)
        #Conterá os units_ids de todos os equipamento conectados no gateway.
                                                 #Equipamentos com mesmo ip
       
        self.modbusClients = []
        self.modbusClients : list[ list[ ClienteModbus ] ]       
        
       
        #Inicializando o cliente MQTT
        self.mqttClient = mqtt.Client(protocol=mqtt.MQTTv5)   
            
            
        self.mqttClient.on_message = self.on_received_message_from_mqtt
        self.mqttClient.on_publish = self.on_publish
        
        self.IsConnected = False
        
        self.ListaTempoDeLeitura = []
                    
        indexClient = 0         
        for currentDeviceName, currentDeviceConfigurations in self.devicesData.items():
            
            #self.modbusclients.append(ClienteModbus(deviceName = currentDeviceName, host = currentDeviceConfigurations["Ip"], 
            #                                        port = currentDeviceConfigurations["Port"], unit_id = 1)) 
            #unit_id = 1 inicialmente; na leitura das variaveis ele muda para cada valor configurado no topico
            
            tags = currentDeviceConfigurations["TagsConfiguration"]
            #tagsOrdenadas = dict(sorted(tags.items(), key = lambda item: ( item[1]["SlaveId"], item[1]["RegisterAddress"]))) # Operação (O n log(n))
            #Pensar em ordenar depois se for o caso de tentar otimizar para mandar um comando para ler já vários endereços modbus
            
            #print(tags)
            #print(tagsOrdenadas)
            
            #"Invertendo" as tags para elas serem armazenadas em un dicionário com o tópico como chave do resto das informações que estaram em um dicionário
            #Isso é para que quando o cliente mqtt receber uma mensagem do broker, ele possa identificar mais rapidamente qual o tópico para obter as
            #informações necessárias para escrita 
            
            ids = {}  #Key = id
            indexClientUnit_id = -1       #Para que no if dentro do for abaixo ele inicie com indexClientUnit_id = 0 após ter sido somado no else
            self.modbusClients.append([])
            self.topics.append([])
            for tagKey, tagValues in tags.items():
                
                currentSlaveId = tagValues["SlaveId"]
                
                if currentSlaveId in ids:
                    topicName = tagValues["Topic"]
                    tagValues["TagName"] = tagKey
                    self.topics[indexClient][indexClientUnit_id][topicName] = tagValues
                else:
                    indexClientUnit_id += 1
                    ids[currentSlaveId] = currentSlaveId
                    
                    self.identification.append( (indexClient, indexClientUnit_id) )
                    
                    modbusClient = ClienteModbus(deviceName = currentDeviceName, host = currentDeviceConfigurations["Ip"], 
                                                port = currentDeviceConfigurations["Port"], unit_id = currentSlaveId,
                                                timeout = 3)
                    self.modbusClients[indexClient].append(modbusClient)
                    
                    topicName = tagValues["Topic"]
                    tagValues["TagName"] = tagKey
                    self.topics[indexClient].append( {topicName: tagValues } )
                    
                    
                    
                    
                #topicName = tagValues["Topic"]
                #tagValues["TagName"] = tagKey
                #self.topics[index][topicName] = tagValues
            
           
            indexClient += 1 
            
       
        
        print(self.identification)
        print(self.modbusClients)
        print(self.topics)
        

    def connection(self):
        for i in self.identification:
            posClientIp = i[0]
            posClientId = i[1]
            
            ClientesModbusCorrespondentes = self.modbusClients[posClientIp][posClientId]
            ClientesModbusCorrespondentes : ClienteModbus
            ClientesModbusCorrespondentes.Connection()
            
      
        try:
            if self.mqttClient.connect(host=self.brokerIp, port=self.brokerPort, keepalive = 60) == 0: 
                print("Successfully connected to MQTT broker")
                #self.mqttClient.loop_start()
                self.subscribeMqtt()
                self.IsConnected = True
            else:
                print("Unable to connect to MQTT broker")
        except Exception as e:
            print("Unable to establish connection with the MQTT broker due to error: ", e)
            
            
    def disconnect(self):
        self.mqttClient.loop_stop()
        self.mqttClient.disconnect()
        self.IsConnected = False
        for i in self.identification:
            posClientIp = i[0]
            posClientId = i[1]
            
            ClientesModbusCorrespondentes = self.modbusClients[posClientIp][posClientId]
            ClientesModbusCorrespondentes:ClienteModbus
            ClientesModbusCorrespondentes.close()
        
        print("Gateway Disconnectado")
            
    
    def modbus2mqtt(self):
        
        for i in self.identification:
            posClientIp = i[0]
            posClientId = i[1]
            
            topicosCorrespondentes = self.topics[posClientIp][posClientId]
            ClientesModbusCorrespondentes = self.modbusClients[posClientIp][posClientId]
            ClientesModbusCorrespondentes : ClienteModbus
            
            #print(ClientesModbusCorrespondentes.is_open)
            if not ClientesModbusCorrespondentes.isconnected:  #Se não estiver conectado iniciará uma thread para reconexão do cliente e passará a leitura para o próximo
                ClientesModbusCorrespondentes.InicializeReconnectionThread(maxReconnectionFailures=3, waitTimeForNewTryOfReconnection=20)
                continue
            
            
            for topicName, topicData in topicosCorrespondentes.items():
                
                tempo_antes = time.perf_counter()
                
                timestamp_inicio_leitura = datetime.now()
                timestamp_inicio_leitura = timestamp_inicio_leitura.strftime("%Y-%m-%d %H:%M:%S.%f")
                modbus_value_read = ClientesModbusCorrespondentes.readTag(address = topicData["RegisterAddress"],
                                                                            functionId = topicData["FunctionId"])
                
                if type(modbus_value_read) == list :
                    modbus_value_read = modbus_value_read[0]
                    
                if modbus_value_read == None:
                    ClientesModbusCorrespondentes.isconnected = False
                    break
                    
                timestamp_apos_leitura_modbus = datetime.now()
                timestamp_apos_leitura_modbus = timestamp_apos_leitura_modbus.strftime("%Y-%m-%d %H:%M:%S.%f")
                    
                payload = {}
                payload["Valor"] = modbus_value_read
                payload["Timestamp de Inicio"] = timestamp_inicio_leitura
                payload["Timestamp apos Leitura"] = timestamp_apos_leitura_modbus
                
                #Transformando no formato json
                payload = json.dumps(payload)
                     
                #self.mqttClient.publish(topic = currentDeviceTagsConfigurations[currentTag]["Topic"], payload = modbus_value_read)
                info = self.mqttClient.publish(topic = topicName, payload = payload)

                if(info.rc == mqtt.MQTT_ERR_SUCCESS):
                    print(f"Mensagem publicada em topic = {topicName}, valor = {modbus_value_read}  ")
                    pass
                
                tempo_depois = time.perf_counter()
                
                self.ListaTempoDeLeitura.append(tempo_depois - tempo_antes)
        
        '''Antigo
        
        for deviceIndex in range(self.num_devices):
            topics = self.topics[deviceIndex]
            
            tempo_antes = time.perf_counter()
            for topicName, topicData in topics.items():
                
                      
                tempo_antes = time.perf_counter()
                
                timestamp_inicio_leitura = datetime.now()
                timestamp_inicio_leitura = timestamp_inicio_leitura.strftime("%Y-%m-%d %H:%M:%S.%f")
                modbus_value_read = self.modbusclients[deviceIndex].readTag(address = topicData["RegisterAddress"],
                                                                            functionId = topicData["FunctionId"],
                                                                            slaveId = topicData["SlaveId"] )
                
                if type(modbus_value_read) == list :
                    modbus_value_read = modbus_value_read[0]
                    
                timestamp_apos_leitura_modbus = datetime.now()
                timestamp_apos_leitura_modbus = timestamp_apos_leitura_modbus.strftime("%Y-%m-%d %H:%M:%S.%f")
                    
                payload = {}
                payload["Valor"] = modbus_value_read
                payload["Timestamp de Inicio"] = timestamp_inicio_leitura
                payload["Timestamp apos Leitura"] = timestamp_apos_leitura_modbus
                
                #Transformando no formato json
                payload = json.dumps(payload)
                     
                #self.mqttClient.publish(topic = currentDeviceTagsConfigurations[currentTag]["Topic"], payload = modbus_value_read)
                info = self.mqttClient.publish(topic = topicName, payload = payload)

                if(info.rc == mqtt.MQTT_ERR_SUCCESS):
                    print(f"Mensagem publicada em topic = {topicName}, valor = {modbus_value_read}  ")
                
                tempo_depois = time.perf_counter()
                
                self.ListaTempoDeLeitura.append(tempo_depois - tempo_antes)
        '''
    
    
    def subscribeMqtt(self):
        """Realiza a subscrição em todos os tópicos passadas no json de configuração e inicia o loop do mqtt client

        """     
        for i in self.identification:
            posClientIp = i[0]
            posClientId = i[1]
            
            topicosCorrespondentes = self.topics[posClientIp][posClientId]
            for currentTopicName in topicosCorrespondentes.keys():
                print(currentTopicName)
                self.mqttClient.subscribe(topic = currentTopicName, options=mqtt.SubscribeOptions(noLocal=True))
                
        self.mqttClient.loop_start()
        
        print("Subscrição nos tópicos realizadas.\n" + 
               "Gateway pronto para inicialização")
                
        
    def on_publish(self, client, userdata, mid):
        #self.publishIds.append(mid)
        pass
    
    def on_received_message_from_mqtt(self, client, user_data, message):
        print("Subscription Recebida")
        
        contain = message.payload.decode()
        
        if contain == "":
            contain = 0
            
        
        topicName = message.topic
        #Pensar se necessário em como parar a publicação do modbus para o mqtt de um tópico específico quando se for publicar nele
        #Uma ideai seria criar uma variavel (estaPublicando) no dicionario de cada topico a qual será verificada na hora da publicação
        #Ai quando entrasse no handler que recebeu uma mensagem esta variavel irá para false, até que o valor seja escrito no modbus e depois voltaria para true
        for i in self.identification:
            posClientIp = i[0]
            posClientId = i[1]
            
            topicosCorrespondentes = self.topics[posClientIp][posClientId]
            ClientesModbusCorrespondentes = self.modbusClients[posClientIp][posClientId]
            ClientesModbusCorrespondentes : ClienteModbus
            
            if topicName in topicosCorrespondentes.keys():
                topicData = topicosCorrespondentes[topicName]
                timestamp_inicio_escrita = datetime.now()
                timestamp_inicio_escrita = timestamp_inicio_escrita.strftime("%Y-%m-%d %H:%M:%S.%f")
                
                ClientesModbusCorrespondentes.writeByConnectionValues(topicData["RegisterAddress"], topicData["FunctionId"],
                                                                     contain)
                
                timestamp_fim_escrita = datetime.now()
                timestamp_fim_escrita = timestamp_fim_escrita.strftime("%Y-%m-%d %H:%M:%S.%f")
                
                payload = {}
                payload["Valor"] = contain
                payload["Timestamp de Inicio escrita"] = timestamp_inicio_escrita
                payload["Timestamp apos escrita"] = timestamp_fim_escrita
                
                #Transformando no formato json
                payload = json.dumps(payload)
                
                self.mqttClient.publish(topic=topicName, payload=payload)
                
                print("Tentando escrever")
                        
        
        print("message contain: " + str(contain))
            
            