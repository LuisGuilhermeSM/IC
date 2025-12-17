from pyModbusTCP.server import ModbusServer, DataBank
import time
from datetime import datetime
import csv
import threading

def startWritingServer():
    x = 0
    
    while threadAtiva and escritaAtiva:
        y = x**2
        
        modServer.data_bank.set_holding_registers(address = 0, word_list= [y])
        
        timestamp = datetime.now()
        timestamp = timestamp.strftime("%Y-%m-%d %H:%M:%S.%f")
        listaValores.append([y, timestamp])
        
        
        x = x + 1
        time.sleep(tempo_de_amostragem)

tempo_de_amostragem = 0.5 #Em segundos

modServer = ModbusServer(host="10.0.0.4", port=502, no_block=True)

print("Inicializando servidor modbus")
modServer.start()
print("Servidor Inicializado")

modServer.data_bank.set_holding_registers(address = 0, word_list= [999])
#arquivo = open(file="ValoresDoModbusServer.csv", mode="w", newline="")
#w = csv.writer(arquivo)

ServerThread = threading.Thread(target=startWritingServer, args=())
threadAtiva = False
escritaAtiva = True


#listaValoresServidor = [] 
listaValores = [ ["valor", "timeStamp" ] ] 

#for i in range(0, 100):
while True:
    cond = int(input("Digite 1 para inicializar a escrita da função\n" + 
                    "Digite 2 para parar a escrita\n" + 
                    "Digite 3 para fechar o servidor\n"))
    if cond == 1:
        threadAtiva = True
        ServerThread.start()

    elif cond == 2:
        escritaAtiva = False
    
    elif cond == 3:
        print("Fechando Servidor")
        threadAtiva = False
        modServer.stop()
        break

#print(listaValoresServidor)
#w.writerows(listaValoresServidor)
#arquivo.close()

#listaValores = [ ["valor", "timeStamp" ] ]  ta em cima

# Abre o arquivo em modo de escrita
with open('Modbus\\ValoresDoModbusServer.csv', 'w', newline='', encoding='utf-8') as arquivo:
    # Cria o escritor CSV
    escritor = csv.writer(arquivo)

    # Escreve os dados
    escritor.writerows(listaValores)
    
print(listaValores)