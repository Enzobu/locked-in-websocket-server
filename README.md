# Locked-In Node WS

Serveur WebSocket Node pour les ESP8266 du projet Locked-In.

## Rôle

- reçoit les connexions des ESP
- enregistre chaque ESP via son `deviceId`
- lit les commandes en attente dans Redis
- envoie les commandes à l'ESP connecté
- stocke les réponses dans Redis pour que Symfony les récupère

## Compatibilité avec ton Symfony actuel

Ce serveur est compatible avec :

- `locker_ws:outbox:<deviceId>`
- `locker_ws:response:<correlationId>`
- payloads du style :
  - commande : `{"type":"command","action":"open","locker":2,"correlationId":"...","deviceId":"esp-main"}`
  - réponse ESP : `{"type":"command_result","locker":2,"status":"opened","correlationId":"..."}`

## Installation

```bash
npm install
cp .env.example .env
npm start
```

## Variables utiles

- `WS_HOST` : host d'écoute, mets `0.0.0.0`
- `WS_PORT` : port WS, par défaut `30174`
- `REDIS_URL` : URL Redis
- `OUTBOX_PREFIX` : préfixe des commandes sortantes
- `RESPONSE_PREFIX` : préfixe des réponses
- `RESPONSE_TTL_SECONDS` : TTL des réponses

## Docker

```bash
docker build -t locked-in-node-ws .
docker run --rm -p 30174:30174 --env-file .env locked-in-node-ws
```

## Compose à brancher avec ton backend

Exemple de service :

```yaml
ws-node:
  build:
    context: ./locked-in-node-ws
    dockerfile: Dockerfile
  container_name: locked-in-node-ws
  environment:
    WS_HOST: 0.0.0.0
    WS_PORT: 30174
    REDIS_URL: redis://redis:6379
  ports:
    - "30174:30174"
  depends_on:
    - redis
  restart: unless-stopped
```

## Côté ESP

- host : IP de la machine Docker
- port : `30174`
- path : `/`

## Logs attendus

Quand un ESP se connecte :

```text
WS listening on ws://0.0.0.0:30174
Connected ::ffff:192.168.1.21
Message {"type":"identify","deviceId":"esp-main","ip":"192.168.1.21"}
Registered device esp-main
```
