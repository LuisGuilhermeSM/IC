from pyModbusTCP.client import ModbusClient
from struct import unpack, pack
import threading
import time

def transposeMatrix(matrix):
    return [
        [int(matrix[j][i]) for j in range(len(matrix))] for i in range(len(matrix[0]))
    ]

class ClienteModbus(ModbusClient):
    def __init__(self, deviceName, **kwargs):
        super().__init__(**kwargs)
        self.deviceName = deviceName
        self.isconnected = False
        
        
        #Values for the reading
        self.qtdTags = 0
        self.slaveIds = 0
        self.functionIds = 0
        self.addresses = 0
        self.valuesRead = [] 
        self.qtdTags = 0
        
        #Valores para reconexão
        self.reconnectionFailures = 0
        self.maxReconnectionFailures = 3
        self.reconnecting = False  #Indica se o cliente está tentando se reconectar pela thread
        self.waitTimeForNewTryOfReconnection = 30 #Em segundos
         
    
    def Connection(self):
        """Test the client modbus connection
        """
        
        #self.open() não retorna erro caso não seja possível realizar a conexão
        
        if(self.open() == True):
            self.isconnected = True
            print(f"TCP connection to the {self.deviceName} device on unit_id = {self.unit_id} is ok")
            #print(f"Self.is_open = {self.is_open}")
            if (self.read_holding_registers(1, 1) == None):
                self.isconnected = False
                print(f"Unable to reach device")
        else:
            self.isconnected = False
            print("Unable to stablish connection to the " + self.deviceName + " device on unit_id = " + str(self.unit_id) )
    
        
    def InicializeReconnectionThread(self, maxReconnectionFailures, waitTimeForNewTryOfReconnection):
        if self.reconnecting:
            
            print(f"Cliente modbus {self.deviceName}, unit_id = {self.unit_id} já está tentado se reconectar")
            print(f"numTentaivas = {self.reconnectionFailures}, self.is_open = {self.is_open}") 
            print(f"Thread ativa: {self.threadReconnection.is_alive()}")
            return
        
        def reconnection():
            while (not self.isconnected):
                if self.reconnectionFailures < self.maxReconnectionFailures:
                    #ComunicacaoTcp = self.open()
                    resposta = self.read_holding_registers(1, 1)
                    
                    if  resposta != None:
                        self.isconnected = True
                        self.reconnecting = False
                        self.reconnectionFailures += 0
                    else:
                        self.reconnectionFailures += 1
                else:
                    print(f"Tentaivas máximas alcançadas para reconexão para o Cliente modbus {self.deviceName}, unit_id = {self.unit_id},\n esperando {self.waitTimeForNewTryOfReconnection} para tentar novamente")
                    self.reconnectionFailures = 0
                    time.sleep(self.waitTimeForNewTryOfReconnection)
                    
        
        self.reconnecting = True 
        self.maxReconnectionFailures = maxReconnectionFailures
        self.waitTimeForNewTryOfReconnection = waitTimeForNewTryOfReconnection
        self.threadReconnection = threading.Thread(target=reconnection, daemon=True)
        self.threadReconnection.start()

    def readTag(self, address, functionId):
        """Reads the Modbus register at the specified address using the specified function code.

        Args:
            address (int): The Modbus register address for reading.
            functionId (int): Represents the function code for reading, where:
                - 1: Reads coils
                - 2: Reads discrete inputs
                - 3: Reads holding registers
                - 4: Reads input registers
                - 5: Reads a float from holding registers

        Returns:
            bool, int, float: The value read from the address register.
        """
        valueRead = None
        
        if(functionId == 1):
            valueRead = self.read_coils(address - 1, 1)
            return valueRead
        elif(functionId == 2):
            valueRead = self.read_discrete_inputs(address - 1, 1)
            return valueRead
        elif(functionId == 3):
            valueRead = self.read_holding_registers(address - 1, 1)
            return valueRead
        elif(functionId == 4):
            valueRead = self.read_input_registers(address -1, 1)
            return valueRead
        elif(functionId == 5):
            valueRead = self.Readfloat(address)
            return valueRead
                 
        
    def writeByConnectionValues(self, address, functionId, value):
        """Writes a value to the Modbus tag represented by the specified indexTag.

        Args:
            address (int): The Modbus register address for writing.
            functionId (int): Represents the function code for reading, where:
                - 1: Write coils
                - 3: Write holding registers
                - 5: Write a float in holding registers
            value (bool, int, float): The value to be written to the register.

        Returns:
            bool: A confirmation message of the write operation. 
                  Note: Even if this returns False, the write may have occurred.
        """
        
        #No teste com o modsim a função de escrita retornava falso, indicando algum problema nela. Mesmo assim, o valor passado era sim escrito.
        #Não era problema de latência pois aumentei o tempo de espera da resposta e ainda retornou falso
        #E pelo wishark confirmei sim que o modsim mandava uma mensagem de resposta
        #Uma possível causa é que a mensagem de confirmação apresentada não está no formato esperado pelo pymodbus e por isso retorna falso 
        #Testei no clp e a função retorna True confirmando sim que houve a escrita e que o problema é no formato da resposta de mensagem do modsim
        
        
        address = int(address)
        functionId = int(functionId)
        
        if(functionId == 1):
            write = self.write_single_coil(address - 1, int(value))
            return write
        elif(functionId == 3):
            write = self.write_single_register(address - 1, int(value))
            return write
        elif(functionId == 5):
            write = self.WriteFloat(address, float(value))
            return write
            
    
    def Readfloat(self, address):
        """Reads two Modbus address registers that are consecutive and converts them to a decimal (float) value.

        Args:
            address (int): The initial Modbus register address.

        Returns:
            None, float: The decimal value read from the consecutive registers, or None if the read fails.
        """
        valueRead = self.read_holding_registers(address - 1, 2)
        if valueRead is None:
            return None
        else:
            return self.ConvertRegistersToFloat(valueRead, order="little")
            
    
    def WriteFloat(self, address, value):
        return self.write_multiple_registers(address - 1, self.ConvertsFloatToRegisters(value) )
        
            
    def ConvertRegistersToFloat(self, values, order = "little"):
        """
    Converte 32 bits passados como dois números inteiros de 16 bits na lista values para um float.
    Os valores de values representam respectivamente a most significant word e lowest significant word.

    Args:
        values (list or tuple): Uma lista ou tupla contendo dois inteiros de 16 bits.
        order (str): Uma string representando a ordem dos bytes:
            - "little": Little-endian (LSW antes de MSW) representando um float.
            - "big": Big-endian (MSW antes de LSW) representando um swappedFloat.

    Returns:
        float: O número de ponto flutuante resultante (32 bits IEEE 754).
    """
        
        if len(values) != 2:
            raise ValueError("A entrada 'values' deve conter exatamente dois valores inteiros de 16 bits.")

        MSW = values[0]
        LSW = values[1]

        if order == "big":
            # Combina os dois valores em big-endian
            combined = (MSW << 16) | LSW
            
        elif order == "little":
            # Combina os dois valores em little-endian
            combined = (LSW << 16) | MSW
        else:
            raise ValueError("O argumento 'order' deve ser 'big' ou 'little'.")
        
        # Converte para float usando struct
        return unpack('>f', combined.to_bytes(4, byteorder='big'))[0]  #struct.unpack()
    
    def ConvertsFloatToRegisters(self, value):
        
        inteiro = unpack(">I", pack(">f", value))[0]
        
        #Separando em big-endian
        MSW = (inteiro >> 16) & 0xFFFF
        LSW = inteiro & 0xFFFF
        
        #Mandando em little-endian
        return [LSW, MSW]
        