from DBHandler import DBClient
import threading

db_client = DBClient(configuration_file = "jsons\\TagsConfig.json" )

def star_aplication():
    while(threadAtiva):
        db_client.run()

interfaceAtiva = True
while(interfaceAtiva):
        entrada = int(input("Digite 1 para iniciar a aplicação \n" +
                        "Digite 2 para parar a aplicação \n" + 
                        "Digite 3 para sair da aplicação \n"))
            
        if(entrada == 1):
            db_client.start()
            #DBThread = threading.Thread(target=star_aplication)
            #threadAtiva = True
            #DBThread.start()
        if(entrada == 2):
            threadAtiva = False
            db_client.stop()
        if(entrada == 3):
            db_client.stop()
            threadAtiva = False
            interfaceAtiva = False
            #GatewayThread.join()