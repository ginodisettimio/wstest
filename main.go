package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"strconv"

	"github.com/coder/websocket"
)

/*

Patrón de Diseño HUB

Conexión:
1. Entra una petición a wsHandler.
2. wsHandler envía la puntero ws al canal hub.register.
3. El select en la goroutine hub.run() recibe la puntero y añade el cliente a h.clients[ws] = true.

Transmisión de Mensajes:
4. Alguien produce un mensaje y lo envía a hub.broadcast.
5. El select en hub.run() despierta e itera sobre h.clients para transmitir el mensaje a cada socket.

Desconexión:
6. El usuario cierra el navegador o pierde internet.
7. ws.Read() falla con un error en wsHandler.
8. El bucle de lectura termina y se ejecuta la cláusula defer func() { hub.unregister <- ws }().
9. El select en hub.run() recibe el puntero y lo elimina con delete(h.clients, ws).

*/

type Hub struct {
	// Mapa de clientes activos. Solo la goroutine 'run()' tiene acceso a él.
	clients map[*websocket.Conn]bool

	// Canal por donde los nuevos clientes solicitan registrarse.
	register chan *websocket.Conn

	// Canal por donde los clientes que se desconectan solicitan salir.
	unregister chan *websocket.Conn

	// Canal para transmitir mensajes entrantes a todos los clientes.
	broadcast chan []byte
}

func newHub() *Hub {
	return &Hub{
		clients:    make(map[*websocket.Conn]bool),
		register:   make(chan *websocket.Conn),
		unregister: make(chan *websocket.Conn),
	}
}

func (h *Hub) run() {
	for {
		select {
		case conn := <-h.register:
			h.clients[conn] = true
			// Notificar nueva conexion
			h.broadcastCount()

		case conn := <-h.unregister:
			if _, ok := h.clients[conn]; ok {
				delete(h.clients, conn)
				// Notificar desconexión
				h.broadcastCount()
			}
		}
	}
}

func (h *Hub) broadcastCount() {
	// Convertir longitud a bytes(uint8)
	countMsg := []byte(strconv.Itoa(len(h.clients)))
	for client := range h.clients {
		// Enviar mensaje a todos los clientes
		err := client.Write(context.Background(), websocket.MessageText, countMsg)
		if err != nil {
			log.Printf("Error transmitiendo a cliente: %v", err)
		}
	}
}

func main() {
	hub := newHub()
	go hub.run()

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		http.ServeFile(w, r, "index.html")
	})

	http.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		wsHandler(hub, w, r)
	})

	fmt.Println("Servidor iniciado en :8080")
	err := http.ListenAndServe(":8080", nil)
	if err != nil {
		log.Fatalf("Error en servidor: %v", err)
	}
}

func wsHandler(hub *Hub, w http.ResponseWriter, r *http.Request) {
	ws, err := websocket.Accept(w, r, nil)
	if err != nil {
		log.Printf("Error al aceptar conexión WebSocket: %v", err)
		return
	}
	defer ws.Close(websocket.StatusNormalClosure, "Conexión finalizada")

	hub.register <- ws
	defer func() {
		hub.unregister <- ws
	}()

	// Mantener la conexión abierta leyendo hasta que el cliente se desconecte
	ctx := r.Context()
	for {
		_, _, err := ws.Read(ctx)
		if err != nil {
			break
		}
	}
}
