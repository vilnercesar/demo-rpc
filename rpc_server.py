# Nome do arquivo: rpc_server.py (Atualizado)

import socketserver
import time
import sys
import threading

# Pega o número da porta a partir do argumento da linha de comando
if len(sys.argv) < 2:
    print("Erro: Forneça o número da porta.")
    print("Uso: python rpc_server.py <porta>")
    sys.exit(1)

PORT = int(sys.argv[1])
HOST = "localhost"

class MeuHandlerTCP(socketserver.BaseRequestHandler):
    def handle(self):
        data = self.request.recv(1024).strip().decode('utf-8')
        thread_name = threading.current_thread().name
        print(f"[Servidor {HOST}:{PORT}, {thread_name}]: Recebeu chamada RPC: '{data}'")

        # --- Lógica de Roteamento ---

        if "ASSINCRONO" in data:
            # 1. Envia ACK imediato
            print(f"[Servidor {HOST}:{PORT}, {thread_name}]: Enviando ACK imediato para '{data}'")
            self.request.sendall("ACK".encode('utf-8'))
            
            # 2. Faz o trabalho demorado DEPOIS
            print(f"[Servidor {HOST}:{PORT}, {thread_name}]: Processando por 2 segundos...")
            time.sleep(2)
            print(f"[Servidor {HOST}:{PORT}, {thread_name}]: Processamento de '{data}' concluído (sem resposta final).")

        elif "UNIDIRECIONAL" in data:
            # Apenas processa, sem ACK e sem resposta
            print(f"[Servidor {HOST}:{PORT}, {thread_name}]: Processando por 2 segundos...")
            time.sleep(2)
            print(f"[Servidor {HOST}:{PORT}, {thread_name}]: Chamada unidirecional concluída (sem resposta).")
            
        else: # Síncrono, Adiado, ou Multicast
            # Processa primeiro, depois envia a resposta final
            print(f"[Servidor {HOST}:{PORT}, {thread_name}]: Processando por 2 segundos...")
            time.sleep(2)
            resposta = f"OK (processado: {data})"
            print(f"[Servidor {HOST}:{PORT}, {thread_name}]: Enviando resposta final.")
            self.request.sendall(resposta.encode('utf-8'))


if __name__ == "__main__":
    try:
        with socketserver.ThreadingTCPServer((HOST, PORT), MeuHandlerTCP) as server:
            print(f"Servidor RPC ouvindo em {HOST}:{PORT}...")
            server.serve_forever()
    except KeyboardInterrupt:
        print("\nServidor desligado.")
    except Exception as e:
        print(f"Erro ao iniciar o servidor na porta {PORT}: {e}")
