# Prueba de Concepto de WebSockets en GO

## Patrón de Diseño HUB

### Conexión:
1. Entra una petición a wsHandler.
2. wsHandler envía la puntero ws al canal hub.register.
3. El select en la goroutine hub.run() recibe la puntero y añade el cliente a h.clients[ws] = true.

### Transmisión de Mensajes:
4. Alguien produce un mensaje y lo envía a hub.broadcast.
5. El select en hub.run() despierta e itera sobre h.clients para transmitir el mensaje a cada socket.

### Desconexión:
6. El usuario cierra el navegador o pierde internet.
7. ws.Read() falla con un error en wsHandler.
8. El bucle de lectura termina y se ejecuta la cláusula defer func() { hub.unregister <- ws }().
9. El select en hub.run() recibe el puntero y lo elimina con delete(h.clients, ws).
