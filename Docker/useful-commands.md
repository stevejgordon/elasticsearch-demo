 Access bash terminal for container:

`docker exec -it docker_es01_1 bash`

Navigate to config/certs/es01

openssl x509 -fingerprint -sha256 -in es01.crt