import socket
import threading
import time


def meu_callback(resposta):
    print(f"\n[CALLBACK EXECUTADO] {time.ctime()}: Resultado final recebido: {resposta}\n")

def rpc_worker_adiado(servidor, porta, mensagem, callback_func):
    print(f"  [Thread Worker Adiado]: {time.ctime()}: Conectando em {servidor}:{porta}...")
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((servidor, porta))
            s.sendall(mensagem.encode('utf-8'))
            
            # 1. Espera o ACK imediato do servidor 
            ack = s.recv(1024).decode('utf-8')
            print(f"  [Thread Worker Adiado]: {time.ctime()}: Recebeu '{ack}'. Thread principal está livre.")
            
            # 2. Faz o trabalho demorado
            print(f"  [Thread Worker Adiado]: {time.ctime()}: Fazendo trabalho local enquanto espera o callback...")
            time.sleep(1) # Simula trabalho
            
            # 3. Espera o RESULTADO FINAL (
            resposta = s.recv(1024).decode('utf-8')
            callback_func(resposta)
            
    except ConnectionRefusedError:
        print(f"  [Thread Worker Adiado]: Erro! Servidor {servidor}:{porta} está offline.")

def rpc_worker_multicast(servidor, porta, mensagem):
    print(f"  [Thread Multicast]: {time.ctime()}: Enviando para {servidor}:{porta}...")
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((servidor, porta))
            s.sendall(mensagem.encode('utf-8'))
            resposta = s.recv(1024).decode('utf-8')
            print(f"  [Thread Multicast]: {time.ctime()}: Resposta de {servidor}:{porta} = {resposta}")
    except ConnectionRefusedError:
        print(f"  [Thread Multicast]: Erro! Servidor {servidor}:{porta} está offline.")


# --- 1. RPC SÍNCRONO (Padrão) ---
def chamada_sincrona(servidor, porta, mensagem):
    print("\n--- TESTANDO 1: RPC SÍNCRONO (Padrão) ---")
    print(f"Cliente: {time.ctime()}: Enviando requisição e BLOQUEANDO até a resposta FINAL...")
    start_time = time.time()
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((servidor, porta))
            s.sendall(mensagem.encode('utf-8'))
            # PONTO DE BLOQUEIO: O cliente espera aqui pelo resultado final (2 segundos)
            resposta = s.recv(1024).decode('utf-8')
            end_time = time.time()
            print(f"Cliente: {time.ctime()}: Resposta recebida: '{resposta}'")
            print(f"Cliente: Tempo total bloqueado: {end_time - start_time:.2f}s (Deve ser > 2s)")
    except ConnectionRefusedError:
        print(f"Cliente: Erro! Servidor {servidor}:{porta} está offline.")


# --- 2. RPC ASSÍNCRONO (Com ACK) ---
def chamada_assincrona(servidor, porta, mensagem):
    print("\n--- TESTANDO 2: RPC ASSÍNCRONO (Com ACK) ---")
    print(f"Cliente: {time.ctime()}: Enviando requisição e BLOQUEANDO apenas pelo ACK...")
    start_time = time.time()
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((servidor, porta))
            s.sendall(mensagem.encode('utf-8'))
            
            # PONTO DE BLOQUEIO: Espera apenas o ACK imediato do servidor
            resposta_ack = s.recv(1024).decode('utf-8')
            
            end_time = time.time()
            print(f"Cliente: {time.ctime()}: Resposta recebida: '{resposta_ack}'")
            print(f"Cliente: Tempo total bloqueado: {end_time - start_time:.2f}s (Deve ser quase 0s)")
            print(f"Cliente: {time.ctime()}: Cliente está livre, embora servidor ainda esteja processando.")
            
    except ConnectionRefusedError:
        print(f"Cliente: Erro! Servidor {servidor}:{porta} está offline.")


# --- 3. RPC UNIDIRECIONAL  ---
def chamada_unidirecional(servidor, porta, mensagem):
    print("\n--- TESTANDO 3: RPC UNIDIRECIONAL (One-Way) ---")
    print(f"Cliente: {time.ctime()}: Enviando requisição e NÃO esperando NADA (nem ACK)...")
    start_time = time.time()
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((servidor, porta))
            s.sendall(mensagem.encode('utf-8'))
           
    except ConnectionRefusedError:
        print(f"Cliente: Erro! Servidor {servidor}:{porta} está offline.")
    end_time = time.time()
    print(f"Cliente: {time.ctime()}: Requisição enviada.")
    print(f"Cliente: Tempo total bloqueado: {end_time - start_time:.2f}s (Deve ser quase 0s)")


# --- 4. RPC SÍNCRONO ADIADO (com Callback) ---
def chamada_sincrona_adiada_callback(servidor, porta, mensagem):
    print(f"\n--- TESTANDO 4: RPC SÍNCRONO ADIADO (CALLBACK) ---")
    print(f"Cliente: {time.ctime()}: Disparando chamada em uma thread separada...")
    
    # Esta é uma implementação simulada. 
    # Em um sistema real, o cliente faria uma chamada assíncrona (como no Teste 2)
    # e o servidor enviaria o resultado final para o cliente *depois*,
    # o que exigiria que o cliente também fosse um servidor.
    # Para simplificar, usamos uma thread para simular o cliente fazendo outras coisas
    # enquanto a thread "worker" espera pelo resultado final.
    
    # Atualizamos o rpc_worker_adiado para lidar com uma lógica de 2 ACKs
    # (ACK imediato, e Resultado Final depois).
    # Para este exemplo, vamos simplificar e assumir que o "ACK" do servidor 
    # no modelo Adiado é, na verdade, o *resultado final* que chega depois.


    print(f"Cliente: {time.ctime()}: (Simulação) A Thread Principal está livre...")
    
    # Para simular o "adiado" (deferred) corretamente:
    # 1. A thread principal envia a chamada.
    # 2. O servidor envia o ACK (que ignoramos na thread principal).
    # 3. A thread principal faz outras coisas.
    # 4. O servidor envia o RESULTADO FINAL (callback) mais tarde.
    

    
    worker = threading.Thread(target=rpc_worker_adiado_simplificado, 
                              args=(servidor, porta, mensagem, meu_callback))
    worker.start()
    
    print(f"Cliente: {time.ctime()}: Thread principal está livre e fazendo outro trabalho...")
    time.sleep(1) # Simula outro trabalho
    print(f"Cliente: {time.ctime()}: Thread principal terminou seu trabalho.")
    print(f"Cliente: {time.ctime()}: Esperando o callback ser disparado pela outra thread...")
    worker.join() # Espera a thread worker terminar
    print(f"Cliente: {time.ctime()}: Demonstração do Adiado/Callback concluída.")

def rpc_worker_adiado_simplificado(servidor, porta, mensagem, callback_func):
    """Worker simplificado para o Teste 4."""
    print(f"  [Thread Worker Adiado]: {time.ctime()}: Conectando em {servidor}:{porta}...")
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((servidor, porta))
            s.sendall(mensagem.encode('utf-8'))
            
            # Bloqueia (nesta thread) esperando a resposta final
            resposta = s.recv(1024).decode('utf-8')
            
            # Chama o callback com o resultado
            callback_func(resposta)
            
    except ConnectionRefusedError:
        print(f"  [Thread Worker Adiado]: Erro! Servidor {servidor}:{porta} está offline.")


# --- 5. RPC MULTICAST ---
def chamada_multicast(servidores, mensagem):
    print(f"\n--- TESTANDO 5: RPC MULTICAST ---")
    print(f"Cliente: {time.ctime()}: Disparando chamadas para {len(servidores)} servidores em paralelo...")
    
    threads = []
    for (host, porta) in servidores:
        t = threading.Thread(target=rpc_worker_multicast, 
                             args=(host, porta, mensagem))
        threads.append(t)
        t.start()
    
    print(f"Cliente: {time.ctime()}: Thread principal esperando todas as respostas do multicast...")
    for t in threads:
        t.join()
    print(f"Cliente: {time.ctime()}: Multicast concluído.")


if __name__ == "__main__":
    
    HOST_PADRAO = "localhost"
    PORTA_PADRAO = 9091
    
    # Teste 1: Requer servidor na porta 9091
    chamada_sincrona(HOST_PADRAO, PORTA_PADRAO, "SINC:Processar_Pedido_A")
    input("\nPressione Enter para o próximo teste...\n")
    
    # Teste 2: Requer servidor na porta 9091
    chamada_assincrona(HOST_PADRAO, PORTA_PADRAO, "ASSINCRONO:Iniciar_Backup_B")
    input("\nPressione Enter para o próximo teste...\n")

    # Teste 3: Requer servidor na porta 9091
    chamada_unidirecional(HOST_PADRAO, PORTA_PADRAO, "UNIDIRECIONAL:Log_Evento_C")
    time.sleep(0.5) # Pausa para o servidor começar a processar o unidirecional
    input("\nPressione Enter para o próximo teste...\n")
    
    # Teste 4: Requer servidor na porta 9091
    chamada_sincrona_adiada_callback(HOST_PADRAO, PORTA_PADRAO, "ADIADO:Calcular_Relatorio_D")
    input("\nPressione Enter para o próximo teste...\n")

    # Teste 5: Requer servidores nas portas 9091, 9092 e 9093
    SERVIDORES_GRUPO = [
        ("localhost", 9091),
        ("localhost", 9092),
        ("localhost", 9093),
    ]
    chamada_multicast(SERVIDORES_GRUPO, "MULTICAST:Atualizar_Cache_E")
    
    print("\n--- Todos os testes concluídos ---")
