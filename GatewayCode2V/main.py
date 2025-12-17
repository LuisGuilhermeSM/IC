from Gateway import ModbusMqttGateway
from DBHandler import DBClient
import threading
import time


if __name__ == '__main__':
    def startGateway(gateway:ModbusMqttGateway):
        ''' Teste anterior
        intervalo = 0.5 #Intervalo entre cada leitura
        last_time = 0
        while(threadAtiva):
            start_time = time.perf_counter()
            if (start_time - last_time >= intervalo):
                gateway.modbus2mqtt()
                last_time = time.perf_counter()
        '''
        
        
        #Teste atual 
        intervalo = 0.5 #Intervalo entre cada leitura
        next_time = time.perf_counter() # Tempo que deve ocorrer a leitura
        while(threadAtiva):
            start_time = time.perf_counter()
            gateway.modbus2mqtt()
            tempo_leitura = time.perf_counter() - start_time
            #print(f"tempo leitura total = {tempo_leitura}")
            tempo_de_espera = max(0, intervalo - tempo_leitura)
            time.sleep(tempo_de_espera)
        
            '''
            next_time += intervalo
            current_time = time.perf_counter()
            print(f"next_time = {next_time}, current_time = {current_time}, next_time - current_time = {next_time - current_time} ")
            if(current_time < next_time): 
                #Caso houve imprecisão no time.sleep() e não se "dormiu" o restante para dar os 0.5s, ou seja, executou em 0.1s e dormiu 0.399
                #então se dormirá mais o tempo necessário para a próxima leitura (next_time - current_time)
                time.sleep(next_time - current_time)
                print("Imprecisão na parada")
                #print(f"next_time = {next_time}, current_time = {current_time}, next_time - current_time = {next_time - current_time} ")
            else:
                #Caso a execução demorou mais que o tempo de amostragem, current_time > next_time, já se executa a próxima leitura
                next_time = current_time + intervalo
                print("Execução demorou mais que o tempo de amostragem")
            print("\n")
            '''
    
    configurationFilePath = "GatewayCode2V\\jsons\\TesteReconexao.json"
    
    gate = ModbusMqttGateway(configurationFilePath) #No VScode
    #gate = ModbusMqttGateway("jsons\\TagsConfig.json") #No Terminal
    
    threadAtiva = False
    
    
    db_client = DBClient(configuration_file = configurationFilePath )
    db_isActive = False
    
    #lista_tempos_leituras = []
    
    #Criação de uma simples interface no terminal para testes
    #GatewayThread = threading.Thread(target=startGateway, args=(gate,))
    interfaceAtiva = True
    while(interfaceAtiva):
        entrada = int(input("Digite 1 para conectar o gateway \n" +
                        "Digite 2 para iniciar o gateway \n"       +
                        "Digite 3 para parar e desconectar o gateway \n" + 
                        "Digite 4 para inciar ou parar o salvamento no Banco de Dados \n" +
                        "Digite 5 para sair da aplicação \n"))
        
        if(entrada == 1):
            if(gate.IsConnected):
                print("Gateway já estava conectado")
            else:
                gate.connection()
        elif (entrada == 2):
            if(gate.IsConnected):
                if threadAtiva:
                    print("O Gateway ja foi inicializado")
                else:
                    GatewayThread = threading.Thread(target=startGateway, args=(gate,), daemon = True)
                    threadAtiva = True
                    GatewayThread.start()
            else:
                print("Gateway não foi conectado não há como inicializa-lo")
        elif(entrada == 3):
            if(gate.IsConnected or threadAtiva):
                threadAtiva = False
                gate.disconnect()
        elif(entrada == 4):
            if(db_isActive):
                db_client.stop()
                db_isActive = False
            else:
                db_client.start()
                db_isActive = True
        elif(entrada == 5):
            threadAtiva = False
            gate.disconnect()
            interfaceAtiva = False
            try:
                print(f"Lista de tempos = {gate.ListaTempoDeLeitura}")
                print(f"Tempo medio = {sum(gate.ListaTempoDeLeitura)/len(gate.ListaTempoDeLeitura)}")
            except Exception as e:
                print("Error in reading json configuration file: ", e)
            #GatewayThread.join()
            
    